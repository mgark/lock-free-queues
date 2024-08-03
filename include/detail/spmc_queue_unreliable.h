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
#include <limits>
#if defined(__x86_64__)

  #include "detail/common.h"
  #include "spmc_queue_base.h"
  #include <atomic>
  #include <type_traits>

template <class T, size_t _MAX_CONSUMER_N_ = 8, size_t _MAX_PRODUCER_N_ = 1, size_t _BATCH_NUM_ = 4, class Allocator = std::allocator<T>>
class SPMCMulticastQueueUnreliable
  : public SPMCMulticastQueueBase<T, SPMCMulticastQueueUnreliable<T, _MAX_CONSUMER_N_, _MAX_PRODUCER_N_, _BATCH_NUM_, Allocator>,
                                  _MAX_CONSUMER_N_, _BATCH_NUM_, Allocator>
{
  static_assert(std::is_trivially_copyable_v<T>);

  struct ProducerContext
  {
    alignas(64) std::atomic<size_t> producer_idx_{std::numeric_limits<size_t>::max()};

    template <class Producer>
    size_t aquire_idx(Producer& p) requires(_MAX_PRODUCER_N_ == 1)
    {
      size_t new_idx = 1 + producer_idx_.load(std::memory_order_acquire);
      producer_idx_.store(new_idx, std::memory_order_release);
      return new_idx;
    }

    template <class Producer>
    size_t aquire_idx(Producer& p) requires(_MAX_PRODUCER_N_ > 1)
    {
      size_t old_idx;
      size_t new_idx;
      do
      {
        old_idx = producer_idx_.load(std::memory_order_acquire);
        new_idx = old_idx + 1;
  #ifdef _TRACE_STATS_
        ++p.stats().cas_num;
  #endif
      } while (!producer_idx_.compare_exchange_strong(old_idx, new_idx, std::memory_order_acq_rel,
                                                      std::memory_order_acquire));
      return new_idx;
    }

    template <class Producer>
    size_t aquire_first_idx(Producer& p) requires(_MAX_PRODUCER_N_ == 1)
    {
      size_t new_idx;
      size_t old_idx = producer_idx_.load(std::memory_order_acquire);
      do
      {
        new_idx = 1 + old_idx;
      } while (!producer_idx_.compare_exchange_strong(old_idx, new_idx, std::memory_order_release,
                                                      std::memory_order_acquire));
      return new_idx;
    }

    template <class Producer>
    size_t aquire_first_idx(Producer& p) requires(_MAX_PRODUCER_N_ > 1)
    {
      return aquire_idx(p);
    }
  };

  ProducerContext producer_ctx_;

public:
  using Base = SPMCMulticastQueueBase<T, SPMCMulticastQueueUnreliable, _MAX_CONSUMER_N_, _BATCH_NUM_, Allocator>;
  using NodeAllocTraits = typename Base::NodeAllocTraits;
  using Node = typename Base::Node;
  using ProducerTicket = typename Base::ProducerTicket;
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
    // we can have as many as consumers as possibly can since the conflated queue does not specifically track consumers
    return {this->size(), 0, 0 /*does not matter basically*/, std::numeric_limits<size_t>::max(), 0, 0, ConsumerAttachReturnCode::Attached};
  }

  /*
    Return value: true - given consumer was locked and now it has been unlocked
                  false - given consumer was already unlocked
  */
  bool detach_consumer(size_t consumer_id) { return true; }

  template <class Producer>
  ProducerTicket attach_producer(Producer& p)
  {
    return {std::numeric_limits<size_t>::max(), 0, ProducerAttachReturnCode::Attached};
  }

  bool detach_producer(size_t producer_id) { return true; }

  template <class Producer, class... Args, bool blocking = Producer::blocking_v>
  ProduceReturnCode emplace(size_t original_idx, Producer& producer, Args&&... args)
  {
    if (!is_running())
    {
      return ProduceReturnCode::NotRunning;
    }

    // the whole point for the producer is just keep publishing to the new
    // slots regardless of where consumers are or at what state they are

    size_t idx = original_idx & this->idx_mask_;
    Node& node = this->nodes_[idx];
    {
      size_t version = node.version_.load(std::memory_order_relaxed);
      node.version_.store(version + 1, std::memory_order_relaxed); // WARNING! indicating that write is in-progress
      std::atomic_thread_fence(std::memory_order_release); // again it is not strictly standard compliant, but will work on
      //  x86. This fence prevents stores preceding it to-reorder with the writes following it, so it means if reader would see
      //  even a single bit of the newly created object, ***it would have to see a version incremented above***!
      void* storage = node.storage_;
      NodeAllocTraits::construct(this->alloc_, static_cast<T*>(storage), std::forward<Args>(args)...);
      node.version_.store(version + 2, std::memory_order_release);
    }

    return ProduceReturnCode::Published;
  }

  template <class C, class F>
  ConsumeReturnCode consume_by_func(size_t idx, size_t& queue_idx, Node& node, size_t version,
                                    C& consumer, F&& f)
  {
    size_t& previous_version = consumer.previous_version_;
    if ((version & 1) == 0 && previous_version < version)
    {
      std::byte tmp[sizeof(T)];
      std::memcpy(tmp, node.storage_, sizeof(T));
      // WARNING! because we don't read atomic, it is not technically standard compliant, but
      // on x86 we get away since the acq fence below prohibits by CPU and compiler to re-order
      // Loads preceding the fence and loads following it. This implies that if any bit of the
      // object gonna be read, its new version would be read as well so that we can detect it if the
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
        return ConsumeReturnCode::Consumed;
      }
      else
      {
        return ConsumeReturnCode::SlowConsumer;
      }
    }

    return ConsumeReturnCode::NothingToConsume;
  }

  template <class C>
  ConsumeReturnCode skip(size_t idx, size_t& queue_idx, Node& node, size_t version, C& consumer)
  {
    size_t& previous_version = consumer.previous_version_;
    if ((version & 1) == 0 && previous_version < version)
    {
      ++consumer.consumer_next_idx_;
      if (idx + 1 == this->n_)
      { // need to rollover
        previous_version = version;
      }
      return ConsumeReturnCode::Consumed;
    }
    else
    {
      return ConsumeReturnCode::NothingToConsume;
    }
  }

  template <class Producer>
  size_t aquire_idx(Producer& p)
  {
    return producer_ctx_.aquire_idx(p);
  }

  template <class Producer>
  size_t aquire_first_idx(Producer& p)
  {
    return producer_ctx_.aquire_first_idx(p);
  }
};

#endif