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

#include "detail/common.h"
#include "detail/producer.h"
#include "detail/spin_lock.h"
#include "spmc_queue_base.h"
#include <atomic>
#include <cassert>
#include <catch2/benchmark/detail/catch_benchmark_stats.hpp>
#include <limits>

template <class T, size_t _MAX_CONSUMER_N_ = 8, size_t _MAX_PRODUCER_N_ = 1, size_t _BATCH_NUM_ = 4, class Allocator = std::allocator<T>>
class SPMCMulticastQueueReliableBounded
  : public SPMCMulticastQueueBase<T, SPMCMulticastQueueReliableBounded<T, _MAX_CONSUMER_N_, _MAX_PRODUCER_N_, _BATCH_NUM_, Allocator>,
                                  _MAX_CONSUMER_N_, _BATCH_NUM_, Allocator>
{
  using Me = SPMCMulticastQueueReliableBounded<T, _MAX_CONSUMER_N_, _MAX_PRODUCER_N_, _BATCH_NUM_, Allocator>;

  struct alignas(64) ConsumerProgress
  {
    std::atomic<size_t> idx;
    std::atomic<size_t> previous_version;
    std::atomic<size_t> queue_idx;
  };

  struct ProducerContext
  {
    alignas(64) std::atomic<size_t> producer_idx_;
    alignas(64) std::atomic<size_t> min_next_consumer_idx_;
    alignas(64) std::atomic<size_t> min_next_producer_idx_;

    struct alignas(64) ProducerProgress
    {
      std::atomic<size_t> idx;
    };

    std::array<ProducerProgress, _MAX_PRODUCER_N_ + 1> producer_progress_;
    alignas(64) std::array<std::atomic<bool>, _MAX_PRODUCER_N_ + 1> producer_registry_;
    alignas(64) Me& host_;

    ProducerContext(Me& host) : host_(host)
    {
      for (auto it = std::begin(producer_progress_); it != std::end(producer_progress_); ++it)
        it->idx.store(PRODUCER_IS_WELCOME, std::memory_order_release);

      std::fill(std::begin(producer_registry_), std::end(producer_registry_), 0 /*unlocked*/);

      producer_idx_.store(std::numeric_limits<size_t>::max(), std::memory_order_release);
      min_next_consumer_idx_.store(CONSUMER_IS_WELCOME, std::memory_order_release);
      min_next_producer_idx_.store(PRODUCER_IS_WELCOME, std::memory_order_release);
    }

    size_t get_min_next_consumer_idx_cached() const
    {
      return min_next_consumer_idx_.load(std::memory_order_acquire);
    }

    size_t get_min_next_producer_idx_cached() const
    {
      return min_next_producer_idx_.load(std::memory_order_acquire);
    }

    void cache_min_next_consumer_idx(size_t idx)
    {
      min_next_consumer_idx_.store(idx, std::memory_order_release);
    }

    void cache_min_next_producer_idx(size_t idx)
    {
      min_next_producer_idx_.store(idx, std::memory_order_release);
    }

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
        host_.producer_ctx_.producer_progress_[p.producer_id_].idx.store(new_idx, std::memory_order_release);
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
        host_.producer_ctx_.producer_progress_[p.producer_id_].idx.store(new_idx, std::memory_order_release);
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

  // using ProducerContext =
  //   std::conditional_t<_MAX_PRODUCER_N_ == 1, ProducerSingleThreadedContext, ProducerSynchronizedContext&>;

  ProducerContext producer_ctx_;

  using ConsumerProgressArray = std::array<ConsumerProgress, _MAX_CONSUMER_N_ + 1>;
  using ConsumerRegistryArray = std::array<std::atomic<bool>, _MAX_CONSUMER_N_ + 1>;

  alignas(64) ConsumerProgressArray consumers_progress_;
  alignas(64) ConsumerRegistryArray consumers_registry_;

  // these variables change somewhat infrequently
  alignas(64) std::atomic<int> consumers_pending_attach_;
  std::atomic<size_t> max_consumer_id_;
  std::atomic<size_t> max_producer_id_;

public:
  using Base =
    SPMCMulticastQueueBase<T, SPMCMulticastQueueReliableBounded, _MAX_CONSUMER_N_, _BATCH_NUM_, Allocator>;
  using NodeAllocTraits = typename Base::NodeAllocTraits;
  using Node = typename Base::Node;
  using ConsumerTicket = typename Base::ConsumerTicket;
  using ProducerTicket = typename Base::ProducerTicket;
  using type = typename Base::type;
  using Base::Base;
  using Base::consume_blocking;
  using Base::is_running;
  using Base::is_stopped;
  using Base::start;
  using Base::stop;

  SPMCMulticastQueueReliableBounded(std::size_t N, const Allocator& alloc = Allocator())
    : Base(N, alloc), producer_ctx_(*this), consumers_pending_attach_(0), max_consumer_id_(0), max_producer_id_(0)
  {
    for (auto it = std::begin(consumers_progress_); it != std::end(consumers_progress_); ++it)
      it->idx.store(CONSUMER_IS_WELCOME, std::memory_order_release);

    std::fill(std::begin(consumers_registry_), std::end(consumers_registry_), 0 /*unlocked*/);
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

          bool producer_need_to_accept = true;
          if (!is_running())
          {
            Spinlock::scoped_lock autolock(this->slow_path_guard_);
            if (!is_running())
            {
              // if the queue has not started, then let's assign the next read idx by ourselves!
              this->consumers_progress_.at(i).idx.store(0, std::memory_order_release);
              this->consumers_progress_.at(i).previous_version.store(0, std::memory_order_release);
              producer_need_to_accept = false;
            }
          }

          if (producer_need_to_accept)
          {
            this->consumers_progress_.at(i).idx.store(CONSUMER_JOIN_REQUESTED, std::memory_order_release);
            this->consumers_pending_attach_.fetch_add(1, std::memory_order_release);
          }

          {
            Spinlock::scoped_lock autolock(this->slow_path_guard_);
            auto latest_max_consumer_id = this->max_consumer_id_.load(std::memory_order_relaxed);
            this->max_consumer_id_.store(std::max(i, latest_max_consumer_id), std::memory_order_release);
          }

          // let's wait for producer to consumer our positions from which we
          // shall start safely consuming
          size_t stopped;
          size_t consumer_next_idx;
          size_t previous_version;
          do
          {
            stopped = is_stopped();
            consumer_next_idx = this->consumers_progress_.at(i).idx.load(std::memory_order_acquire);
            previous_version = this->consumers_progress_.at(i).previous_version.load(std::memory_order_relaxed);
          } while (!stopped && consumer_next_idx >= CONSUMER_JOIN_INPROGRESS);

          if (stopped)
          {
            detach_consumer(i);
            return {0,
                    0,
                    CONSUMER_IS_WELCOME,
                    this->items_per_batch_,
                    0,
                    0,
                    ConsumerAttachReturnCode::Stopped};
          }

          return {this->size(), i, consumer_next_idx, this->items_per_batch_, 0, previous_version, ConsumerAttachReturnCode::Attached};
        } // else someone stole the locker just before us!
      }
    }

    // not enough space for the new consumer!
    return {0, 0, CONSUMER_IS_WELCOME, this->items_per_batch_, 0, 0, ConsumerAttachReturnCode::ConsumerLimitReached};
  }

  bool detach_consumer(size_t consumer_id)
  {
    std::atomic<bool>& locker = this->consumers_registry_.at(consumer_id);
    bool is_locked = locker.load(std::memory_order_acquire);
    if (is_locked)
    {
      this->consumers_progress_.at(consumer_id).idx.store(CONSUMER_IS_WELCOME, std::memory_order_release);
      if (locker.compare_exchange_strong(is_locked, false, std::memory_order_release, std::memory_order_relaxed))
      {
        // this->consumers_pending_dettach_.fetch_add(1, std::memory_order_release);
        //  technically even possible that while we unregistered it another
        //  thread, another consumer stole our slot legitimately and we just
        //  removed it from the registery, effectively leaking it...
        //  so unlocked the same consumer on multiple threads is really a bad
        //  idea

        Spinlock::scoped_lock autolock(this->slow_path_guard_);
        // It shall be safe to update max consumer idx as the attach function would restore max consumer idx shall one appear right after.
        auto new_max_consumer_id = _MAX_CONSUMER_N_;
        while (new_max_consumer_id > 0)
        {
          if (this->consumers_progress_.at(new_max_consumer_id).idx.load(std::memory_order_relaxed) == CONSUMER_IS_WELCOME)
            --new_max_consumer_id;
          else
          {
            break;
          }
        }

        this->max_consumer_id_.store(new_max_consumer_id, std::memory_order_release);
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

  size_t try_accept_new_consumer(size_t i, size_t consumer_from_idx)
  {
    size_t consumer_next_idx = this->consumers_progress_[i].idx.load(std::memory_order_acquire);
    if (CONSUMER_JOIN_REQUESTED == consumer_next_idx)
    {
      // new consumer wants a ticket!
      if (this->consumers_progress_[i].idx.compare_exchange_strong(
            consumer_next_idx, CONSUMER_JOIN_INPROGRESS, std::memory_order_acquire, std::memory_order_relaxed))
      {
        consumer_next_idx = consumer_from_idx;
        size_t previous_version = consumer_next_idx / this->size();
        this->consumers_progress_[i].previous_version.store(previous_version, std::memory_order_release);
        this->consumers_progress_[i].idx.store(consumer_next_idx, std::memory_order_release);
        this->consumers_pending_attach_.fetch_sub(1, std::memory_order_release);
      }
    }
    return consumer_next_idx;
  }

  template <class Producer>
  ProducerTicket attach_producer(Producer& p)
  {
    for (size_t i = 1; i < _MAX_PRODUCER_N_ + 1; ++i)
    {
      std::atomic<bool>& locker = this->producer_ctx_.producer_registry_.at(i);
      bool is_locked = locker.load(std::memory_order_acquire);
      if (!is_locked)
      {
        if (locker.compare_exchange_strong(is_locked, true, std::memory_order_release, std::memory_order_relaxed))
        {
          {
            Spinlock::scoped_lock autolock(this->slow_path_guard_);
            auto latest_max_producer_id = this->max_producer_id_.load(std::memory_order_relaxed);
            this->max_producer_id_.store(std::max(i, latest_max_producer_id), std::memory_order_release);
          }

          // let's wait for producer to consumer our positions from which we
          // shall start safely consuming
          size_t stopped = is_stopped();
          if (stopped)
          {
            detach_producer(i);
            return {0, 0, ProducerAttachReturnCode::Stopped};
          }

          return {i, this->items_per_batch_, ProducerAttachReturnCode::Attached};
        } // else someone stole the locker just before us!
      }
    }

    // not enough space for the new consumer!
    return {0, 0, ProducerAttachReturnCode::ProducerLimitReached};
  }

  bool detach_producer(size_t producer_id)
  {
    std::atomic<bool>& locker = this->producer_ctx_.producer_registry_.at(producer_id);
    bool is_locked = locker.load(std::memory_order_acquire);
    if (is_locked)
    {
      this->producer_ctx_.producer_progress_.at(producer_id).idx.store(PRODUCER_IS_WELCOME, std::memory_order_release);
      if (locker.compare_exchange_strong(is_locked, false, std::memory_order_release, std::memory_order_relaxed))
      {
        // this->consumers_pending_dettach_.fetch_add(1, std::memory_order_release);
        //  technically even possible that while we unregistered it another
        //  thread, another consumer stole our slot legitimately and we just
        //  removed it from the registery, effectively leaking it...
        //  so unlocked the same consumer on multiple threads is really a bad
        //  idea

        Spinlock::scoped_lock autolock(this->slow_path_guard_);
        // It shall be safe to update max consumer idx as the attach function would restore max consumer idx shall one appear right after.
        auto new_max_producer_id = _MAX_PRODUCER_N_;
        while (new_max_producer_id > 0)
        {
          if (this->producer_ctx_.producer_progress_.at(new_max_producer_id).idx.load(std::memory_order_relaxed) ==
              PRODUCER_IS_WELCOME)
            --new_max_producer_id;
          else
          {
            break;
          }
        }

        this->max_producer_id_.store(new_max_producer_id, std::memory_order_release);
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

  template <class Producer>
  void accept_producer(Producer& p)
  {
    size_t expect_welcome = PRODUCER_IS_WELCOME;
    if (!this->producer_ctx_.producer_progress_[p.producer_id_].idx.compare_exchange_strong(
          expect_welcome, p.last_producer_idx_, std::memory_order_acquire, std::memory_order_relaxed))
    {
      throw std::runtime_error("producer is not welcomed!");
    }
  }

  template <class Producer>
  void try_update_producer_progress(size_t original_idx, Producer& p) requires(_MAX_PRODUCER_N_ > 1)
  {
    this->producer_ctx_.producer_progress_[p.producer_id_].idx.store(PRODUCER_JOINED, std::memory_order_release);
  }

  template <class Producer, class... Args, bool blocking = Producer::blocking_v>
  ProduceReturnCode emplace(size_t original_idx, Producer& producer, Args&&... args)
  {
    if (!is_running())
    {
      return ProduceReturnCode::NotRunning;
    }

    size_t min_next_consumer_idx = producer_ctx_.get_min_next_consumer_idx_cached();
    bool no_active_consumers = min_next_consumer_idx == CONSUMER_IS_WELCOME;

    size_t min_next_producer_idx;
    bool slow_producer;
    bool no_free_slot;
    bool slow_consumer =
      min_next_consumer_idx > original_idx || (original_idx - min_next_consumer_idx >= this->n_);
    if constexpr (_MAX_PRODUCER_N_ > 1)
    {
      min_next_producer_idx = producer_ctx_.get_min_next_producer_idx_cached();
      bool no_active_producers = min_next_producer_idx == PRODUCER_IS_WELCOME;
      slow_producer =
        min_next_producer_idx > original_idx || (original_idx - min_next_producer_idx >= this->n_);
      no_free_slot = no_active_consumers || no_active_producers || slow_consumer || slow_producer;
    }
    else
    {
      no_free_slot = no_active_consumers || slow_consumer;
    }

    while (no_free_slot || this->consumers_pending_attach_.load(std::memory_order_acquire))
    {
      size_t min_next_producer_idx_local = original_idx;
      if constexpr (_MAX_PRODUCER_N_ > 1)
      {
        auto max_producer_id = this->max_producer_id_.load(std::memory_order_acquire);
        for (size_t i = 1; i <= max_producer_id; ++i)
        {
          min_next_producer_idx = this->producer_ctx_.producer_progress_[i].idx.load(std::memory_order_acquire);
          if (min_next_producer_idx < PRODUCER_JOINED)
          {
            min_next_producer_idx_local = std::min(min_next_producer_idx_local, min_next_producer_idx);
          }
        }

        assert(min_next_producer_idx_local != PRODUCER_IS_WELCOME);
      }

      size_t min_next_consumer_idx_local = CONSUMER_IS_WELCOME;
      auto max_consumer_id = this->max_consumer_id_.load(std::memory_order_acquire);
      for (size_t i = 1; i <= max_consumer_id; ++i)
      {
        size_t consumer_next_idx = try_accept_new_consumer(i, min_next_producer_idx_local);
        if (consumer_next_idx < CONSUMER_JOIN_INPROGRESS)
        {
          min_next_consumer_idx_local = std::min(min_next_consumer_idx_local, consumer_next_idx);
        }
      }

      producer_ctx_.cache_min_next_consumer_idx(min_next_consumer_idx_local);
      no_active_consumers = min_next_consumer_idx_local == CONSUMER_IS_WELCOME;
      slow_consumer = original_idx - min_next_consumer_idx_local >= this->n_;

      if constexpr (_MAX_PRODUCER_N_ > 1)
      {
        producer_ctx_.cache_min_next_producer_idx(min_next_producer_idx_local);
        slow_producer = original_idx - min_next_producer_idx_local >= this->n_;
        no_free_slot = (no_active_consumers || slow_consumer || slow_producer);
      }
      else
      {
        no_free_slot = (no_active_consumers || slow_consumer);
      }

      if (no_active_consumers)
      {
        return ProduceReturnCode::NoConsumers;
      }

      assert(original_idx >= min_next_consumer_idx_local);

      if constexpr (_MAX_PRODUCER_N_ > 1)
      {
        assert(original_idx >= min_next_producer_idx_local);
      }

      if (!is_running())
      {
        return ProduceReturnCode::NotRunning;
      }

      if constexpr (!blocking)
      {
        if (slow_consumer)
        {
          return ProduceReturnCode::SlowConsumer;
        }

        if constexpr (_MAX_PRODUCER_N_ > 1)
        {
          if (slow_producer)
          {
            return ProduceReturnCode::SlowPublisher;
          }
        }
      }
    }

    size_t idx = original_idx & this->idx_mask_;
    Node& node = this->nodes_[idx];
    size_t orig_version = node.version_.load(std::memory_order_acquire);
    size_t version;

    if constexpr (_MAX_PRODUCER_N_ > 1)
    {
      // cannot estimate properly version as consumer can join / detach dynamically...
      version = original_idx / this->size();
    }
    else
    {
      version = orig_version;
    }

    void* storage = node.storage_;
    if constexpr (!std::is_trivially_destructible_v<T>)
    {
      if (version > 0)
      {
        NodeAllocTraits::destroy(this->alloc_, static_cast<T*>(storage));
      }
    }

    NodeAllocTraits::construct(this->alloc_, static_cast<T*>(storage), std::forward<Args>(args)...);
    node.version_.store(1 + version, std::memory_order_release);
    if constexpr (_MAX_PRODUCER_N_ > 1)
    {
      try_update_producer_progress(original_idx, producer);
    }
#ifdef _TRACE_STATS_
    ++producer.stats().pub_num;
#endif
    return ProduceReturnCode::Published;
  }

  template <class C, class F>
  ConsumeReturnCode consume_by_func(size_t idx, size_t& queue_idx, Node& node, size_t version,
                                    C& consumer, F&& f)
  {
    if (consumer.previous_version_ < version)
    {
      std::forward<F>(f)(node.storage_);
      if (idx + 1 == this->n_)
      { // need to
        // rollover
        consumer.previous_version_ = version;
      }

      ++consumer.consumer_next_idx_;
      if (consumer.consumer_next_idx_ == consumer.next_checkout_point_idx_)
      {
        this->consumers_progress_[consumer.consumer_id_].idx.store(consumer.consumer_next_idx_,
                                                                   std::memory_order_release);
        consumer.next_checkout_point_idx_ = consumer.consumer_next_idx_ + this->items_per_batch_;
      }

      return ConsumeReturnCode::Consumed;
    }
    else
    {
      return ConsumeReturnCode::NothingToConsume;
    }
  }

  template <class C>
  ConsumeReturnCode skip(size_t idx, size_t& queue_idx, Node& node, size_t version, C& consumer)
  {
    if (consumer.previous_version_ < version)
    {
      if (idx + 1 == this->n_)
      { // need to
        // rollover
        consumer.previous_version_ = version;
      }
      ++consumer.consumer_next_idx_;
      if (consumer.consumer_next_idx_ == consumer.next_checkout_point_idx_)
      {
        this->consumers_progress_[consumer.consumer_id_].idx.store(consumer.consumer_next_idx_,
                                                                   std::memory_order_release);
        consumer.next_checkout_point_idx_ = consumer.consumer_next_idx_ + this->items_per_batch_;
      }
      return ConsumeReturnCode::Consumed;
    }
    else
    {
      return ConsumeReturnCode::NothingToConsume;
    }
  }
};