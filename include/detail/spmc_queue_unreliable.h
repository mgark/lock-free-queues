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
#include <cstdint>
#include <limits>
#include <sys/types.h>
#if defined(__x86_64__)

  #include "detail/common.h"
  #include "spmc_queue_base.h"
  #include <atomic>
  #include <type_traits>

template <class T, size_t _MAX_CONSUMER_N_ = 8, size_t _MAX_PRODUCER_N_ = 1, size_t _BATCH_NUM_ = 4,
          class Allocator = std::allocator<T>, class VersionType = size_t>
class SPMCMulticastQueueUnreliable
  : public SPMCMulticastQueueBase<T, SPMCMulticastQueueUnreliable<T, _MAX_CONSUMER_N_, _MAX_PRODUCER_N_, _BATCH_NUM_, Allocator>,
                                  _MAX_CONSUMER_N_, _MAX_PRODUCER_N_, _BATCH_NUM_, 0, true, Allocator, VersionType>
{
public:
  using Base = SPMCMulticastQueueBase<T, SPMCMulticastQueueUnreliable, _MAX_CONSUMER_N_,
                                      _MAX_PRODUCER_N_, _BATCH_NUM_, 0, true, Allocator, VersionType>;
  using Base::_synchronized_consumer_;
  using Base::CPU_PAUSE_N;

private:
  static_assert(std::is_trivially_copyable_v<T>);

  struct ConsumerContext
  {
    template <class Consumer>
    size_t acquire_idx(Consumer& c) requires(!_synchronized_consumer_)
    {
      return c.consumer_next_idx_;
    }

    size_t get_next_idx() const requires(!_synchronized_consumer_) { return CONSUMER_IS_WELCOME; }
  };

  struct ProducerContext
  {
    alignas(_CACHE_PREFETCH_SIZE_) std::atomic<size_t> producer_idx_;

    ProducerContext() { producer_idx_.store(0, std::memory_order_release); }

    template <class Producer>
    size_t aquire_idx(Producer& p) requires(_MAX_PRODUCER_N_ == 1)
    {
      // for single producer relaxed ordering should be enough since this would be
      // called after first successful publishing already done by this producer which
      // would establish the right ordering with previous producer
      size_t new_idx = producer_idx_.load(std::memory_order_relaxed);
      producer_idx_.store(new_idx + 1, std::memory_order_relaxed);
      return new_idx;
    }

    template <class Producer>
    size_t aquire_idx(Producer& p) requires(_MAX_PRODUCER_N_ > 1)
    {
      return producer_idx_.fetch_add(1u, std::memory_order_acq_rel);
    }

    template <class Producer>
    size_t aquire_first_idx(Producer& p) requires(_MAX_PRODUCER_N_ == 1)
    {
      size_t new_idx;
      size_t old_idx = producer_idx_.load(std::memory_order_acquire);
      do
      {
        new_idx = old_idx + 1;
      } while (!producer_idx_.compare_exchange_strong(old_idx, new_idx, std::memory_order_acq_rel,
                                                      std::memory_order_acquire));
      return old_idx;
    }

    template <class Producer>
    size_t aquire_first_idx(Producer& p) requires(_MAX_PRODUCER_N_ > 1)
    {
      return aquire_idx(p);
    }
  };

  ProducerContext producer_ctx_;
  ConsumerContext consumer_ctx_;

public:
  using version_type = VersionType;
  using NodeAllocTraits = typename Base::NodeAllocTraits;
  using Node = typename Base::Node;
  using ProducerTicket = typename Base::ProducerTicket;
  using ConsumerTicket = typename Base::ConsumerTicket;
  using type = typename Base::type;
  using Base::Base;
  using Base::consume_blocking;

  template <class Consumer>
  size_t acquire_consumer_idx(Consumer& c)
  {
    return consumer_ctx_.acquire_idx(c);
  }

  // TODO: rethink attach logic here, probably should give the latest value
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
    // the whole point for the producer is just keep publishing to the new
    // slots regardless of where consumers are or at what state they are

    size_t idx = original_idx & this->idx_mask_;
    Node& node = this->nodes_[idx];
    {
      auto version = node.version_.load(std::memory_order_relaxed);
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

  /*TODO: WARNING! NOT SUPPORTED for Unreliable queues, but maybe we can simulate it by always
  copying a top element!

  template <class C> const T* peek(Node& node, auto version, C& consumer) const
  {
    if (consumer.previous_version_ < version)
    {
      return reinterpret_cast<const T*>(node.storage_);
    }
    else
    {
      return nullptr;
    }
  }*/

  template <class C, class F>
  ConsumeReturnCode consume_by_func(size_t idx, size_t& queue_idx, Node& node, auto version, C& consumer, F&& f)
  {
    auto& previous_version = consumer.previous_version_;
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
      auto recent_version = node.version_.load(std::memory_order_relaxed);
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
  ConsumeReturnCode skip(size_t idx, size_t& queue_idx, Node& node, auto version, C& consumer)
  {
    auto& previous_version = consumer.previous_version_;
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