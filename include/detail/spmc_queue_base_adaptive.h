/*
 * Copyright(c) 2023-present Mykola Garkusha.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "common.h"
#include "spin_lock.h"
#include <atomic>
#include <memory>
#include <string>

template <class T, class Derived, size_t _MAX_CONSUMER_N_ = 8, size_t _BATCH_NUM_ = 4, class Allocator = std::allocator<T>>
class SPMCMulticastQueueAdaptiveBase
{
public:
  using type = T;

  struct ConsumerTicket
  {
    size_t n;
    size_t consumer_id;
    size_t consumer_next_idx;
    size_t items_per_batch;
    size_t queue_idx;
    size_t previous_version;
    ConsumerAttachReturnCode ret_code;
  };

protected:
  struct Node
  {
    std::atomic<size_t> version_{0};
    alignas(T) std::byte storage_[sizeof(T)];
  };

  using NodeAllocator = typename std::allocator_traits<Allocator>::template rebind_alloc<Node>;
  using NodeAllocTraits = std::allocator_traits<NodeAllocator>;
  static_assert(std::is_default_constructible_v<Node>, "Node must be default constructible");

  // these variables pretty much don't change throug the lifetime of the queue
  size_t initial_sz_;
  size_t current_sz_;
  size_t max_sz_;
  size_t items_per_batch_;
  size_t idx_mask_;
  size_t max_outstanding_non_consumed_items_;
  size_t max_queue_num_;
  size_t capacity_;
  NodeAllocator alloc_;
  std::vector<Node*> nodes_;
  Spinlock slow_path_guard_;
  std::atomic<size_t> front_buffer_idx_;
  std::atomic<size_t> back_buffer_idx_;

  // these variables update quite frequently
  std::atomic<QueueState> state_{QueueState::Created};

public:
  SPMCMulticastQueueAdaptiveBase(std::size_t initial_sz, std::size_t max_sz, const Allocator& alloc = Allocator())
    : initial_sz_(initial_sz),
      current_sz_(initial_sz_),
      max_sz_(max_sz),
      items_per_batch_(initial_sz_ / _BATCH_NUM_),
      max_queue_num_(std::max<size_t>(1, std::log2<size_t>(max_sz_ / initial_sz_))),
      idx_mask_(initial_sz - 1),
      max_outstanding_non_consumed_items_((_BATCH_NUM_ - 1) * items_per_batch_),
      alloc_(alloc),
      front_buffer_idx_(0),
      back_buffer_idx_(0)
  {
    // simple geometric sum give that ratio is always 2!
    capacity_ = initial_sz_ * (POWER_OF_TWO[max_queue_num_] - 1);

    if ((initial_sz_ & (initial_sz_ - 1)) != 0)
      throw std::runtime_error("initial_sz not power of two");

    if ((max_sz_ & (max_sz_ - 1)) != 0)
      throw std::runtime_error("max_sz not power of two");

    if (max_outstanding_non_consumed_items_ + items_per_batch_ != initial_sz_)
    {
      throw std::runtime_error(std::string("max_outstanding_non_consumed_items_[")
                                 .append(std::to_string(max_outstanding_non_consumed_items_))
                                 .append("] ")
                                 .append("is NOT equal n_[")
                                 .append(std::to_string(initial_sz_))
                                 .append("] items_per_batch [")
                                 .append(std::to_string(items_per_batch_))
                                 .append("]"));
    }

    if ((items_per_batch_ & (items_per_batch_ - 1)) != 0)
    {
      throw std::runtime_error("items_per_batch_ is not power of two");
    }

    nodes_.resize(max_queue_num_, nullptr);
    nodes_[0] = std::allocator_traits<NodeAllocator>::allocate(alloc_, initial_sz_);
    std::uninitialized_default_construct(nodes_[0], nodes_[0] + initial_sz_);
  }

  ~SPMCMulticastQueueAdaptiveBase()
  {
    stop();

    for (size_t queue_idx = 0; queue_idx < nodes_.size(); ++queue_idx)
    {
      if (nodes_[queue_idx] == nullptr)
        continue;

      if constexpr (!std::is_trivially_destructible_v<T>)
      {
        for (size_t i = 0; i < this->initial_sz_ * POWER_OF_TWO[queue_idx]; ++i)
        {
          Node& node = this->nodes_[queue_idx][i];
          void* storage = node.storage_;
          size_t version = node.version_.load(std::memory_order_acquire);
          if (version > 0)
          {
            NodeAllocTraits::destroy(alloc_, static_cast<T*>(storage));
          }
        }
      }

      NodeAllocTraits::deallocate(alloc_, nodes_[queue_idx], initial_sz_ * POWER_OF_TWO[queue_idx]);
    }
  }

  void stop() { this->state_.store(QueueState::Stopped, std::memory_order_release); }

  bool is_stopped() const
  {
    return this->state_.load(std::memory_order_acquire) == QueueState::Stopped;
  }
  bool is_running() const
  {
    return this->state_.load(std::memory_order_acquire) == QueueState::Running;
  }

  size_t max_queue_num() const { return max_queue_num_; }

  void increment_queue_size(size_t new_sz)
  {
    current_sz_ = new_sz;
    idx_mask_ = current_sz_ - 1;
    items_per_batch_ = current_sz_ / _BATCH_NUM_;
    size_t new_idx = 1 + this->back_buffer_idx_.load(std::memory_order_acquire);
    this->back_buffer_idx_.store(new_idx, std::memory_order_release);
  }

  void start()
  {
    // we need an exclusive lock here as the consumers which join would
    // need to set their own consumer reader idx to 0 if the queue has not started yet - they can
    // only do that before start method is called
    Spinlock::scoped_lock autolock(slow_path_guard_);
    this->state_.store(QueueState::Running, std::memory_order_release);
  }

  template <class C>
  requires std::is_trivially_copyable_v<T> ConsumeReturnCode consume_raw_blocking(size_t& queue_idx,
                                                                                  void* dest, C& consumer)
  {
    QueueState state;
    size_t version;
    ConsumeReturnCode r;
    Node* node;

    do
    {
      size_t idx = consumer.consumer_next_idx_ & consumer.idx_mask_;
      node = &this->nodes_[queue_idx][idx];
      version = node->version_.load(std::memory_order_acquire);
      r = static_cast<Derived&>(*this).consume_by_func(
        idx, queue_idx, *node, version, consumer,
        [dest](void* storage)
        { std::memcpy(dest, std::launder(reinterpret_cast<T*>(storage)), sizeof(T)); });
      if (r == ConsumeReturnCode::Consumed)
        return r;

      state = this->state_.load(std::memory_order_acquire);
    } while (state == QueueState::Running && r == ConsumeReturnCode::NothingToConsume);

    if (state == QueueState::Stopped)
      return ConsumeReturnCode::Stopped;

    return r;
  }

  template <class C>
  ConsumeReturnCode consume_raw_non_blocking(size_t& queue_idx, void* dest, C& consumer)
  {
    size_t idx = consumer.consumer_next_idx_ & consumer.idx_mask_;
    Node& node = this->nodes_[queue_idx][idx];
    size_t version = node.version_.load(std::memory_order_acquire);
    size_t prev_queue_idx = queue_idx;
    auto r = static_cast<Derived&>(*this).consume_by_func(
      idx, queue_idx, node, version, consumer,
      [dest](void* storage)
      { std::memcpy(dest, std::launder(reinterpret_cast<const T*>(storage)), sizeof(T)); });

    if (r == ConsumeReturnCode::Consumed)
    {
      return r;
    }
    else
    {
      if (r == ConsumeReturnCode::NothingToConsume && prev_queue_idx != queue_idx)
      {
        // won't be max 2 calls in the worse case!
        return consume_raw_non_blocking(queue_idx, dest, consumer);
      }

      QueueState state = this->state_.load(std::memory_order_acquire);
      if (state == QueueState::Stopped)
      {
        return ConsumeReturnCode::Stopped;
      }

      return r;
    }
  }

  template <class C, class F>
  ConsumeReturnCode consume_blocking(size_t& queue_idx, C& consumer, F&& f)
  {
    size_t version;
    ConsumeReturnCode r;
    QueueState state;
    Node* node;

    do
    {
      size_t idx = consumer.consumer_next_idx_ & consumer.idx_mask_;
      node = &this->nodes_[queue_idx][idx];
      version = node->version_.load(std::memory_order_acquire);
      r = static_cast<Derived&>(*this).consume_by_func(
        idx, queue_idx, *node, version, consumer,
        [f = std::forward<F>(f)](void* storage) mutable
        { std::forward<F>(f)(*std::launder(reinterpret_cast<const T*>(storage))); });
      if (r == ConsumeReturnCode::Consumed)
        return r;

      state = this->state_.load(std::memory_order_acquire);
    } while (state == QueueState::Running && r == ConsumeReturnCode::NothingToConsume);

    if (state == QueueState::Stopped)
      return ConsumeReturnCode::Stopped;

    return r;
  }

  template <class C, class F>
  ConsumeReturnCode consume_non_blocking(size_t& queue_idx, C& consumer, F&& f)
  {
    size_t idx = consumer.consumer_next_idx_ & consumer.idx_mask_;
    Node& node = this->nodes_[queue_idx][idx];
    size_t prev_queue_idx = queue_idx;
    size_t version = node.version_.load(std::memory_order_acquire);
    auto r = static_cast<Derived&>(*this).consume_by_func(
      idx, queue_idx, node, version, consumer,
      [f = std::forward<F>(f)](void* storage) mutable
      { std::forward<F>(f)(*std::launder(reinterpret_cast<const T*>(storage))); });

    if (r == ConsumeReturnCode::Consumed)
    {
      return r;
    }
    else
    {
      if (r == ConsumeReturnCode::NothingToConsume && prev_queue_idx != queue_idx)
      {
        // won't be max 2 calls in the worse case!
        // first call consume_by_func won't move (f) if there is an item readily available to consume!
        return consume_non_blocking(queue_idx, consumer, std::forward<F>(f));
      }

      QueueState state = this->state_.load(std::memory_order_acquire);
      if (state == QueueState::Stopped)
        return ConsumeReturnCode::Stopped;

      return r;
    }
  }

  template <class C>
  const T* peek_blocking(size_t& queue_idx, C& consumer) const
  {
    Node* node;
    bool running = false;
    const T* r;
    do
    {
      size_t idx = consumer.consumer_next_idx_ & consumer.idx_mask_;
      node = this->nodes_[queue_idx][idx];
      size_t version = node->version_.load(std::memory_order_acquire);
      r = peek(queue_idx, *node, version, consumer);
      if (r)
      {
        return r;
      }
      running = this->is_running();
    } while (running);
    return nullptr;
  }

  template <class C>
  const T* peek_non_blocking(size_t& queue_idx, C& consumer) const
  {
    size_t idx = consumer.consumer_next_idx_ & consumer.idx_mask_;
    Node& node = this->nodes_[queue_idx][idx];
    size_t last_queue_idx = queue_idx;
    size_t version = node.version_.load(std::memory_order_acquire);
    const T* r = peek(queue_idx, node, version, consumer);
    if (last_queue_idx != queue_idx)
    {
      return peek_non_blocking(queue_idx, consumer);
    }
    else
    {
      return r;
    }
  }

  template <class C>
  ConsumeReturnCode skip_blocking(size_t& queue_idx, C& consumer)
  {
    ConsumeReturnCode r;
    size_t version;
    Node* node;
    QueueState state;

    do
    {
      size_t idx = consumer.consumer_next_idx_ & consumer.idx_mask_;
      node = this->nodes_[queue_idx][idx];
      version = node->version_.load(std::memory_order_acquire);
      r = static_cast<Derived&>(*this).skip(idx, queue_idx, *node, version, consumer);
      if (ConsumeReturnCode::Consumed == r)
        return r;

      state = this->state_.load(std::memory_order_acquire);
    } while (state == QueueState::Running && r == ConsumeReturnCode::NothingToConsume);
    if (state == QueueState::Stopped)
      return ConsumeReturnCode::Stopped;

    return r;
  }

  template <class C>
  ConsumeReturnCode skip_non_blocking(size_t& queue_idx, C& consumer)
  {
    size_t idx = consumer.consumer_next_idx_ & consumer.idx_mask_;
    Node& node = this->nodes_[queue_idx][idx];
    size_t prev_queue_idx = queue_idx;
    size_t version = node.version_.load(std::memory_order_acquire);
    auto r = static_cast<Derived&>(*this).skip(idx, queue_idx, node, version, consumer);
    if (r == ConsumeReturnCode::Consumed)
    {
      return r;
    }
    else
    {
      if (r == ConsumeReturnCode::NothingToConsume && prev_queue_idx != queue_idx)
        return skip_non_blocking(queue_idx, consumer);

      QueueState state = this->state_.load(std::memory_order_acquire);
      if (state == QueueState::Stopped)
        return ConsumeReturnCode::Stopped;

      return r;
    }
  }

  size_t size() const { return current_sz_; }
  size_t capacity() const { return capacity_; }

  template <class C>
  bool empty(size_t& queue_idx, C& consumer)
  {
    size_t idx = consumer.consumer_next_idx_ & consumer.idx_mask_;
    Node& node = this->nodes_[queue_idx][idx];
    size_t version = node.version_.load(std::memory_order_acquire);
    size_t last_queue_idx = this->back_buffer_idx_.load(std::memory_order_acquire);
    return (consumer.previous_version_ >= version && queue_idx == last_queue_idx) || !is_running();
  }

  template <class C>
  const T* peek(size_t& queue_idx, Node& node, size_t version, C& consumer) const
  {
    size_t idx = consumer.consumer_next_idx_ & consumer.idx_mask_;
    if (consumer.previous_version_ < version)
    {
      return reinterpret_cast<const T*>(node.storage_);
    }
    else
    {
      size_t last_queue_idx = this->back_queue_idx.load(std::memory_order_acquire);
      if (last_queue_idx > queue_idx)
        queue_idx = last_queue_idx;

      return nullptr;
    }
  }
};
