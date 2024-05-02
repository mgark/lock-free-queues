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

template <class T, class Derived, ProducerKind producerKind = ProducerKind::Unordered,
          size_t _MAX_CONSUMER_N_ = 8, size_t _BATCH_NUM_ = 4, class Allocator = std::allocator<T>>
class SPMCBoundedQueueBase
{
public:
  using type = T;

  struct ConsumerTicket
  {
    size_t consumer_id;
    size_t consumer_next_idx_;
    size_t items_per_batch;
  };

protected:
  struct Node
  {
    std::atomic<size_t> version_{0};
    alignas(T) std::byte storage_[sizeof(T)];
  };

  struct ProducerContextUnordered
  {
    alignas(128) size_t producer_idx_{0};
    size_t aquire_idx() { return producer_idx_++; }
    void release_idx() { --producer_idx_; }
  };

  struct ProducerContextSequencial
  {
    alignas(128) std::atomic<size_t> producer_idx_{0};
    size_t aquire_idx() { return producer_idx_.fetch_add(1, std::memory_order_acquire); }
    void release_idx() { producer_idx_.fetch_sub(1, std::memory_order_acquire); }
  };

  using NodeAllocator = typename std::allocator_traits<Allocator>::template rebind_alloc<Node>;
  using NodeAllocTraits = std::allocator_traits<NodeAllocator>;

  using ProducerContext =
    std::conditional_t<producerKind == ProducerKind::Unordered, ProducerContextUnordered, ProducerContextSequencial>;

  static_assert(std::is_default_constructible_v<Node>, "Node must be default constructible");

  size_t n_;
  size_t items_per_batch_;
  std::atomic<QueueState> state_{QueueState::Created};
  size_t idx_mask_;

  ProducerContext producer_ctx_;
  alignas(128) Node* nodes_;
  alignas(128) std::atomic<int> consumers_pending_attach_;

  using ConsumerProgressType = std::array<std::atomic<size_t>, _MAX_CONSUMER_N_ + 1>;
  using ConsumerRegistryType = std::array<std::atomic<bool>, _MAX_CONSUMER_N_ + 1>;
  alignas(128) ConsumerProgressType consumers_progress_;
  alignas(128) ConsumerRegistryType consumers_registry_;

  size_t max_outstanding_non_consumed_items_;
  NodeAllocator alloc_;
  Spinlock start_guard_;

public:
  SPMCBoundedQueueBase(std::size_t N, const Allocator& alloc = Allocator())
    : n_(N),
      items_per_batch_(n_ / _BATCH_NUM_),
      idx_mask_(n_ - 1),
      consumers_pending_attach_(0),
      max_outstanding_non_consumed_items_((_BATCH_NUM_ - 1) * items_per_batch_),
      alloc_(alloc)
  {
    if ((N & (N - 1)) != 0)
    {
      throw std::runtime_error("N is not power of two");
    }

    if (max_outstanding_non_consumed_items_ + items_per_batch_ != n_)
    {

      throw std::runtime_error(std::string("max_outstanding_non_consumed_items_[")
                                 .append(std::to_string(max_outstanding_non_consumed_items_))
                                 .append("] ")
                                 .append("is NOT equal n_[")
                                 .append(std::to_string(n_))
                                 .append("] items_per_batch [")
                                 .append(std::to_string(items_per_batch_))
                                 .append("]"));
    }

    if ((items_per_batch_ & (items_per_batch_ - 1)) != 0)
    {
      throw std::runtime_error("items_per_batch_ is not power of two");
    }

    std::fill(std::begin(consumers_progress_), std::end(consumers_progress_), CONSUMER_IS_WELCOME);
    std::fill(std::begin(consumers_registry_), std::end(consumers_registry_), 0 /*unlocked*/);

    nodes_ = std::allocator_traits<NodeAllocator>::allocate(alloc_, N);
    std::uninitialized_default_construct(nodes_, nodes_ + N);
  }

  ~SPMCBoundedQueueBase()
  {
    stop();

    if constexpr (!std::is_trivially_destructible_v<T>)
    {
      for (size_t i = 0; i < this->n_; ++i)
      {
        Node& node = this->nodes_[i];
        void* storage = node.storage_;
        size_t version = node.version_.load(std::memory_order_acquire);
        if (version > 0)
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
    Spinlock::scoped_lock autolock(start_guard_);
    this->state_.store(QueueState::Running, std::memory_order_release);
  }

  template <class C>
  requires std::is_trivially_copyable_v<T> ConsumerReturnCode consume_raw_blocking(size_t idx, void* dest, C& consumer)
  {
    QueueState state;
    size_t version;
    ConsumerReturnCode r;
    Node& node = this->nodes_[idx];

    do
    {
      version = node.version_.load(std::memory_order_acquire);
      r = static_cast<Derived&>(*this).consume_by_func(
        idx, node, version, consumer,
        [dest](void* storage)
        { std::memcpy(dest, std::launder(reinterpret_cast<T*>(storage)), sizeof(T)); });
      if (r == ConsumerReturnCode::Consumed)
        return r;

      state = this->state_.load(std::memory_order_acquire);
    } while (state == QueueState::Running && r == ConsumerReturnCode::NothingToConsume);

    if (state == QueueState::Stopped)
      return ConsumerReturnCode::Stopped;

    return r;
  }

  template <class C>
  ConsumerReturnCode consume_raw_non_blocking(size_t idx, void* dest, C& consumer)
  {
    Node& node = this->nodes_[idx];
    size_t version = node.version_.load(std::memory_order_acquire);
    auto r = static_cast<Derived&>(*this).consume_by_func(
      idx, node, version, consumer,
      [dest](void* storage)
      { std::memcpy(dest, std::launder(reinterpret_cast<const T*>(storage)), sizeof(T)); });

    if (r == ConsumerReturnCode::Consumed)
    {
      return r;
    }
    else
    {
      QueueState state = this->state_.load(std::memory_order_acquire);
      if (state == QueueState::Stopped)
        return ConsumerReturnCode::Stopped;

      return r;
    }
  }

  template <class C, class F>
  ConsumerReturnCode consume_blocking(size_t idx, C& consumer, F&& f)
  {
    size_t version;
    ConsumerReturnCode r;
    QueueState state;
    Node& node = this->nodes_[idx];

    do
    {
      version = node.version_.load(std::memory_order_acquire);
      r = static_cast<Derived&>(*this).consume_by_func(
        idx, node, version, consumer,
        [f = std::forward<F>(f)](void* storage) mutable
        { std::forward<F>(f)(*std::launder(reinterpret_cast<const T*>(storage))); });
      if (r == ConsumerReturnCode::Consumed)
        return r;

      state = this->state_.load(std::memory_order_acquire);
    } while (state == QueueState::Running && r == ConsumerReturnCode::NothingToConsume);

    if (state == QueueState::Stopped)
      return ConsumerReturnCode::Stopped;

    return r;
  }

  template <class C, class F>
  ConsumerReturnCode consume_non_blocking(size_t idx, C& consumer, F&& f)
  {
    Node& node = this->nodes_[idx];
    size_t version = node.version_.load(std::memory_order_acquire);
    auto r = static_cast<Derived&>(*this).consume_by_func(
      idx, node, version, consumer,
      [f = std::forward<F>(f)](void* storage) mutable
      { std::forward<F>(f)(*std::launder(reinterpret_cast<const T*>(storage))); });

    if (r == ConsumerReturnCode::Consumed)
    {
      return r;
    }
    else
    {
      QueueState state = this->state_.load(std::memory_order_acquire);
      if (state == QueueState::Stopped)
        return ConsumerReturnCode::Stopped;

      return r;
    }
  }

  template <class C>
  const T* peek_blocking(size_t idx, C& consumer) const
  {
    Node& node = this->nodes_[idx];
    bool stopped = false;
    const T* r;
    do
    {
      size_t version = node.version_.load(std::memory_order_acquire);
      stopped = this->stopped_.load(std::memory_order_acquire);
      r = peek(idx, node, version, consumer);
      if (r)
      {
        return r;
      }
    } while (!stopped);
    return nullptr;
  }

  template <class C, class F>
  const T* peek_non_blocking(size_t idx, C& consumer) const
  {
    Node& node = this->nodes_[idx];
    size_t version = node.version_.load(std::memory_order_acquire);
    return peek(idx, node, version, consumer);
  }

  template <class C>
  ConsumerReturnCode skip_blocking(size_t idx, C& consumer)
  {
    ConsumerReturnCode r;
    size_t version;
    Node& node = this->nodes_[idx];
    QueueState state;

    do
    {
      version = node.version_.load(std::memory_order_acquire);
      r = static_cast<Derived&>(*this).skip(idx, node, version, consumer);
      if (ConsumerReturnCode::Consumed == r)
        return r;

      state = this->state_.load(std::memory_order_acquire);
    } while (state == QueueState::Running && r == ConsumerReturnCode::NothingToConsume);
    if (state == QueueState::Stopped)
      return ConsumerReturnCode::Stopped;

    return r;
  }

  template <class C>
  ConsumerReturnCode skip_non_blocking(size_t idx, C& consumer)
  {
    Node& node = this->nodes_[idx];
    size_t version = node.version_.load(std::memory_order_acquire);
    auto r = static_cast<Derived&>(*this).skip(idx, node, version, consumer);
    if (r == ConsumerReturnCode::Consumed)
    {
      return r;
    }
    else
    {
      QueueState state = this->state_.load(std::memory_order_acquire);
      if (state == QueueState::Stopped)
        return ConsumerReturnCode::Stopped;

      return r;
    }
  }

  size_t size() const { return this->n_; }

  size_t aquire_idx() { return this->producer_ctx_.aquire_idx(); }

  template <class C>
  bool empty(size_t idx, C& consumer)
  {
    Node& node = this->nodes_[idx];
    size_t version = node.version_.load(std::memory_order_acquire);
    return consumer.previous_version_ >= version;
  }

  template <class C>
  const T* peek(size_t idx, Node& node, size_t version, C& consumer) const
  {

    if (consumer.previous_version_ < version)
    {
      return reinterpret_cast<const T*>(node.storage_);
    }
    else
    {
      return nullptr;
    }
  }
};
