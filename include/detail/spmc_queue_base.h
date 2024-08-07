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
#include <cstdint>
#include <memory>
#include <string>

template <class T, class Derived, size_t _MAX_CONSUMER_N_, size_t _BATCH_NUM_, class Allocator, class VersionType>
class SPMCMulticastQueueBase
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
    VersionType previous_version;
    ConsumerAttachReturnCode ret_code;
  };

  struct ProducerTicket
  {
    size_t producer_id;
    size_t items_per_batch;
    ProducerAttachReturnCode ret_code;
  };

protected:
  struct Node
  {
    std::atomic<VersionType> version_{0};
    alignas(T) std::byte storage_[sizeof(T)];
  };

  using NodeAllocator = typename std::allocator_traits<Allocator>::template rebind_alloc<Node>;
  using NodeAllocTraits = std::allocator_traits<NodeAllocator>;
  static_assert(std::is_default_constructible_v<Node>, "Node must be default constructible");

  // these variables pretty much don't change through the lifetime of the queue
  size_t n_;
  size_t items_per_batch_;
  size_t idx_mask_;
  size_t max_outstanding_non_consumed_items_;
  NodeAllocator alloc_;
  Node* nodes_;
  Spinlock slow_path_guard_;

  // these variables update quite frequently
  std::atomic<QueueState> state_{QueueState::Created};

public:
  SPMCMulticastQueueBase(std::size_t N, const Allocator& alloc = Allocator())
    : n_(N), items_per_batch_(n_ / _BATCH_NUM_), idx_mask_(n_ - 1), alloc_(alloc)
  {
    if ((N & (N - 1)) != 0)
    {
      throw std::runtime_error("N is not power of two");
    }

    if ((items_per_batch_ & (items_per_batch_ - 1)) != 0)
    {
      throw std::runtime_error("items_per_batch_ is not power of two");
    }

    nodes_ = std::allocator_traits<NodeAllocator>::allocate(alloc_, N);
    std::uninitialized_default_construct(nodes_, nodes_ + N);
  }

  ~SPMCMulticastQueueBase()
  {
    stop();

    if constexpr (!std::is_trivially_destructible_v<T>)
    {
      size_t producer_last_idx = static_cast<const Derived*>(this)->get_producer_idx();
      for (size_t i = 0; i < this->n_; ++i)
      {
        Node& node = this->nodes_[i];
        void* storage = node.storage_;
        if (i < producer_last_idx + 1)
        {
          NodeAllocTraits::destroy(alloc_, static_cast<T*>(storage));
        }
      }
    }

    NodeAllocTraits::deallocate(alloc_, nodes_, n_);
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
    ConsumeReturnCode r;
    size_t idx = consumer.consumer_next_idx_ & consumer.idx_mask_;
    Node& node = this->nodes_[idx];

    do
    {
      auto version = node.version_.load(std::memory_order_acquire);
      r = static_cast<Derived&>(*this).consume_by_func(
        idx, queue_idx, node, version, consumer,
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
    Node& node = this->nodes_[idx];
    auto version = node.version_.load(std::memory_order_acquire);
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
      QueueState state = this->state_.load(std::memory_order_acquire);
      if (state == QueueState::Stopped)
        return ConsumeReturnCode::Stopped;

      return r;
    }
  }

  template <class C, class F>
  ConsumeReturnCode consume_blocking(size_t& queue_idx, C& consumer, F&& f)
  {
    ConsumeReturnCode r;
    QueueState state;
    size_t idx = consumer.consumer_next_idx_ & consumer.idx_mask_;
    Node& node = this->nodes_[idx];

    do
    {
      auto version = node.version_.load(std::memory_order_acquire);
      r = static_cast<Derived&>(*this).consume_by_func(
        idx, queue_idx, node, version, consumer,
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
    Node& node = this->nodes_[idx];
    auto version = node.version_.load(std::memory_order_acquire);
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
      QueueState state = this->state_.load(std::memory_order_acquire);
      if (state == QueueState::Stopped)
        return ConsumeReturnCode::Stopped;

      return r;
    }
  }

  template <class C>
  const T* peek_blocking(size_t queue_idx, C& consumer) const
  {
    size_t idx = consumer.consumer_next_idx_ & consumer.idx_mask_;
    Node& node = this->nodes_[idx];
    bool running = false;
    const T* r;
    do
    {
      auto version = node.version_.load(std::memory_order_acquire);
      r = peek(queue_idx, node, version, consumer);
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
    Node& node = this->nodes_[idx];
    auto version = node.version_.load(std::memory_order_acquire);
    return peek(queue_idx, node, version, consumer);
  }

  template <class C>
  ConsumeReturnCode skip_blocking(size_t& queue_idx, C& consumer)
  {
    ConsumeReturnCode r;
    size_t idx = consumer.consumer_next_idx_ & consumer.idx_mask_;
    Node& node = this->nodes_[idx];
    QueueState state;

    do
    {
      auto version = node.version_.load(std::memory_order_acquire);
      r = static_cast<Derived&>(*this).skip(idx, queue_idx, node, version, consumer);
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
    Node& node = this->nodes_[idx];
    auto version = node.version_.load(std::memory_order_acquire);
    auto r = static_cast<Derived&>(*this).skip(idx, queue_idx, node, version, consumer);
    if (r == ConsumeReturnCode::Consumed)
    {
      return r;
    }
    else
    {
      QueueState state = this->state_.load(std::memory_order_acquire);
      if (state == QueueState::Stopped)
        return ConsumeReturnCode::Stopped;

      return r;
    }
  }

  size_t capacity() const { return this->n_; }
  size_t size() const { return capacity(); }

  template <class C>
  bool empty(size_t& queue_idx, C& consumer)
  {
    return consumer.consumer_next_idx_ >= static_cast<const Derived*>(this)->get_producer_idx() ||
      !is_running();
  }

  template <class C>
  const T* peek(size_t& queue_idx, Node& node, auto version, C& consumer) const
  {
    return static_cast<const Derived*>(this)->peek(node, version, consumer);
  }
};
