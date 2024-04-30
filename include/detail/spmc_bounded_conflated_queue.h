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

// Confalted queue only supported on x86
#if defined(__x86_64__)

  #include "detail/common.h"
  #include "spmc_bounded_queue_base.h"
  #include <atomic>
  #include <type_traits>

template <class T, ProducerKind producerKind = ProducerKind::Unordered,
          size_t _MAX_CONSUMER_N_ = 16, size_t _BATCH_NUM_ = 4, class Allocator = std::allocator<T>>
class SPMCBoundedConflatedQueue
  : public SPMCBoundedQueueBase<T, SPMCBoundedConflatedQueue<T, producerKind, _MAX_CONSUMER_N_, _BATCH_NUM_, Allocator>,
                                producerKind, _MAX_CONSUMER_N_, _BATCH_NUM_, Allocator>
{
  static_assert(std::is_trivially_copyable_v<T>);

public:
  using Base =
    SPMCBoundedQueueBase<T, SPMCBoundedConflatedQueue, producerKind, _MAX_CONSUMER_N_, _BATCH_NUM_, Allocator>;
  using NodeAllocTraits = typename Base::NodeAllocTraits;
  using Node = typename Base::Node;
  using ConsumerTicket = typename Base::ConsumerTicket;
  using type = typename Base::type;
  using Base::Base;
  using Base::consume_blocking;
  using Base::is_running;
  using Base::is_stopped;
  using Base::start;
  using Base::stop;

  template <class Consumer>
  ConsumerTicket attach_consumer(Consumer& c)
  {
    for (size_t i = 1; i < _MAX_CONSUMER_N_ + 1; ++i)
    {
      std::atomic<bool>& locker = this->consumers_registry_.at(i);
      bool is_locked = locker.load(std::memory_order_acquire);
      if (!is_locked)
      {
        if (locker.compare_exchange_strong(is_locked, true, std::memory_order_release, std::memory_order_relaxed))
        {
          if (is_stopped())
          {
            throw std::runtime_error("queue has been stoppped");
          }

          return {i, 0 /*does not matter basically*/};
        } // else someone stole the locker just before us!
      }
    }

    return {0, CONSUMER_IS_WELCOME};
  }

  /*
    Return value: true - given consumer was locked and now it has been unlcoked
                  false - given consumer was already unlocked
  */
  bool detach_consumer(size_t consumer_id)
  {
    std::atomic<bool>& locker = this->consumers_registry_.at(consumer_id);
    bool is_locked = locker.load(std::memory_order_acquire);
    if (is_locked)
    {
      if (locker.compare_exchange_strong(is_locked, false, std::memory_order_release, std::memory_order_relaxed))
      {
        // consumers_pending_dettach_.fetch_add(1, std::memory_order_release);
        //  technically even possible that while we unregistered it another
        //  thread, another consumer stole our slot legitimately and we just
        //  removed it from the registery, effectively leaking it...
        //  so unlocked the same consumer on multiple threads is really a bad
        //  idea
        return true;
      }
      else
      {

        throw std::runtime_error(
          "unregister_consumer called on another thread "
          "for the same consumer at the same time!");
      }
    }

    return false;
  }

  template <class Producer, class... Args>
  ProducerReturnCode emplace(Producer& producer, Args&&... args)
  {
    if (!is_running())
    {
      return ProducerReturnCode::NotRunning;
    }

    // the whole point for the producer is just keep publishing to the new
    // slots regardless of where consumers are or at what state they are

    size_t original_idx = producer.producer_idx_;
    size_t idx = original_idx & this->idx_mask_;
    Node& node = this->nodes_[idx];
    {
      size_t version = node.version_.load(std::memory_order_relaxed);
      node.version_.store(version + 1, std::memory_order_relaxed); // WARNING! indicating that write is in-progress
      std::atomic_thread_fence(std::memory_order_release); // again it is not striclty standard compliant, but will work on
      //  x86. This fence prevents stores preceeding it to-reorder with the writes following it, so it means if reader would see
      //  even a single bit of the newly created object, ***it would have to see a version incremented above***!
      void* storage = node.storage_;
      NodeAllocTraits::construct(this->alloc_, static_cast<T*>(storage), std::forward<Args>(args)...);
      node.version_.store(version + 2, std::memory_order_release);
    }

    return ProducerReturnCode::Published;
  }

  template <class C, class F>
  ConsumerReturnCode consume_by_func(size_t idx, Node& node, size_t version, C& consumer, F&& f)
  {
    size_t& previous_version = consumer.previous_version_;
    if ((version & 1) == 0 && previous_version < version)
    {
      std::byte tmp[sizeof(T)];
      std::memcpy(tmp, node.storage_, sizeof(T));
      // WARNING! because we don't read atomic, it is not technically standard compliant, but
      // on x86 we get away since the acq fence below prohibts by CPU and compiler to re-order Loads
      // preceeding the fence and loads following it. This implies that if any bit of the object
      // gonna be read, its new version would be read as well so that we can detect it if the
      // producer could warp around and modify our object while we were reading it
      std::atomic_thread_fence(std::memory_order_acquire);
      size_t recent_version = node.version_.load(std::memory_order_relaxed);
      if (recent_version == version)
      {
        std::forward<F>(f)(tmp);
        ++consumer.consumer_next_idx_;
        if (idx + 1 == this->n_)
        { // need to rollover
          previous_version = version;
        }
        return ConsumerReturnCode::Consumed;
      }
      else
      {
        return ConsumerReturnCode::SlowConsumer;
      }
    }

    return ConsumerReturnCode::NothingToConsume;
  }

  template <class C>
  ConsumerReturnCode skip(size_t idx, Node& node, size_t version, C& consumer)
  {
    size_t& previous_version = consumer.previous_version_;
    if ((version & 1) == 0 && previous_version < version)
    {
      ++consumer.consumer_next_idx_;
      if (idx + 1 == this->n_)
      { // need to rollover
        previous_version = version;
      }
      return ConsumerReturnCode::Consumed;
    }
    else
    {
      return ConsumerReturnCode::NothingToConsume;
    }
  }

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

#endif