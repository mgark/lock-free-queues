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
#include <catch2/catch_version.hpp>
#include <cstdint>
#include <emmintrin.h>
#include <limits>
#include <stdexcept>
#include <sys/types.h>
#include <unistd.h>

template <class T, size_t _MAX_CONSUMER_N_ = 8, size_t _MAX_PRODUCER_N_ = 1, size_t _BATCH_NUM_ = 4,
          size_t _CPU_PAUSE_N_ = 0, bool _MULTICAST_ = true, class Allocator = std::allocator<T>,
          class VersionType = std::conditional_t<(!_MULTICAST_ && (_MAX_CONSUMER_N_ > 1)), size_t, uint8_t>>
class SPMCMulticastQueueReliableBounded
  : public SPMCMulticastQueueBase<T, SPMCMulticastQueueReliableBounded<T, _MAX_CONSUMER_N_, _MAX_PRODUCER_N_, _BATCH_NUM_, _CPU_PAUSE_N_, _MULTICAST_, Allocator>,
                                  _MAX_CONSUMER_N_, _MAX_PRODUCER_N_, _BATCH_NUM_, _CPU_PAUSE_N_, _MULTICAST_, Allocator, VersionType>
{
public:
  using Base = SPMCMulticastQueueBase<T, SPMCMulticastQueueReliableBounded, _MAX_CONSUMER_N_, _MAX_PRODUCER_N_,
                                      _BATCH_NUM_, _CPU_PAUSE_N_, _MULTICAST_, Allocator, VersionType>;
  using Base::_batch_consumption_enabled_;
  using Base::_binary_version_;
  using Base::_cache_line_num_;
  using Base::_item_per_prefetch_num_;
  using Base::_remap_index_;
  using Base::CPU_PAUSE_N;
  // using Base::_reuse_single_bit_from_object_;
  using Base::_spsc_;
  using Base::_synchronized_consumer_;
  using Base::_synchronized_producer_;
  using Base::_versionless_;

private:
  using Me =
    SPMCMulticastQueueReliableBounded<T, _MAX_CONSUMER_N_, _MAX_PRODUCER_N_, _BATCH_NUM_, _CPU_PAUSE_N_, _MULTICAST_, Allocator, VersionType>;

  static_assert(_MAX_CONSUMER_N_ > 0);
  static_assert(_MAX_PRODUCER_N_ > 0);

  struct ConsumerContext
  {
    alignas(_CACHE_PREFETCH_SIZE_) std::atomic<size_t> consumer_idx_;

    struct alignas(_CACHE_PREFETCH_SIZE_) ConsumerProgress
    {
      std::atomic<size_t> idx;
      std::atomic<VersionType> previous_version;
    };

    using ConsumerProgressArray = std::array<ConsumerProgress, _MAX_CONSUMER_N_>;
    using ConsumerRegistryArray = std::array<std::atomic<bool>, _MAX_CONSUMER_N_>;

    using ConsumerProgressType = ConsumerProgressArray;
    // std::conditional_t<_MAX_CONSUMER_N_ == 1, ConsumerProgress, ConsumerProgressArray>;
    ConsumerProgressType consumers_progress_;
    ConsumerRegistryArray consumers_registry_;

    // these variables change somewhat infrequently, it got type int to account for race
    // conditions where producers would see first indication of a consumer joining before increment of this variable
    std::atomic<int> consumers_pending_attach_;

    ConsumerContext() : consumer_idx_(0), consumers_pending_attach_(0)
    {
      if constexpr (_MAX_CONSUMER_N_ == 1)
      {
        this->consumers_progress_[0].idx.store(CONSUMER_IS_WELCOME, std::memory_order_release);
      }
      else
      {
        for (auto it = std::begin(consumers_progress_); it != std::end(consumers_progress_); ++it)
          it->idx.store(CONSUMER_IS_WELCOME, std::memory_order_release);
      }

      std::fill(std::begin(consumers_registry_), std::end(consumers_registry_), 0 /*unlocked*/);
    }

    size_t get_next_idx() const requires(!_synchronized_consumer_) { return CONSUMER_IS_WELCOME; }
    size_t get_next_idx() const requires(_synchronized_consumer_)
    {
      return consumer_idx_.load(std::memory_order_acquire);
    }

    template <class Consumer>
    size_t acquire_idx(Consumer& c) requires(!_synchronized_consumer_)
    {
      return c.consumer_next_idx_;
    }

    template <class Consumer>
    size_t acquire_idx(Consumer& c) requires(_synchronized_consumer_)
    {
      if (c.consumer_next_idx_ == NEXT_CONSUMER_IDX_NEEDED)
      {
        // as next_consumer_idx is recorded in the local consumer context, this would help
        // non-blocking consumers to safely resume consumption
        c.consumer_next_idx_ = acquire_idx(c.consumer_id_);
      }

      return c.consumer_next_idx_;
    }

    size_t acquire_idx(size_t consumer_id) requires(_synchronized_consumer_)
    {
      // let's lock in our consumer next idx to the least possible idx so that
      // producer would not overrun us
      size_t idx = consumer_idx_.load(std::memory_order_acquire);
      if constexpr (_MAX_CONSUMER_N_ == 1)
      {
        consumers_progress_[0].idx.store(idx, std::memory_order_release);
      }
      else
      {
        consumers_progress_[consumer_id].idx.store(idx, std::memory_order_release);
      }

      size_t last_idx = consumer_idx_.fetch_add(1, std::memory_order_acq_rel);
      if (idx < last_idx)
      {
        // we can improve our next consumer idx given we know global last consumer idx
        if constexpr (_MAX_CONSUMER_N_ == 1)
        {
          consumers_progress_[0].idx.store(last_idx, std::memory_order_release);
        }
        else
        {
          consumers_progress_[consumer_id].idx.store(last_idx, std::memory_order_release);
        }
      }

      return last_idx;
    }
  };

  struct ProducerContext
  {
    struct alignas(_CACHE_PREFETCH_SIZE_) ProducerProgress
    {
      std::atomic<size_t> idx;
      std::atomic<bool> halt;
    };

    alignas(_CACHE_PREFETCH_SIZE_) std::atomic<size_t> producer_idx_;
    std::array<ProducerProgress, _MAX_PRODUCER_N_> producer_progress_;
    std::array<std::atomic<bool>, _MAX_PRODUCER_N_> producer_registry_;

    ProducerContext()
    {
      for (auto it = std::begin(producer_progress_); it != std::end(producer_progress_); ++it)
        it->idx.store(PRODUCER_IS_WELCOME, std::memory_order_release);

      std::fill(std::begin(producer_registry_), std::end(producer_registry_), 0 /*unlocked*/);
      producer_idx_.store(0, std::memory_order_release);
    }

    template <class Producer>
    size_t aquire_idx(Producer& p) requires(_MAX_PRODUCER_N_ == 1)
    {
      return producer_idx_.load(std::memory_order_acquire);
    }

    template <class Producer>
    size_t aquire_idx(Producer& p) requires(_MAX_PRODUCER_N_ > 1)
    {

      // let's lock in our producer next idx to the least possible idx so that
      // producer would not overrun us
      size_t idx = producer_idx_.load(std::memory_order_acquire);
      producer_progress_[p.producer_id_].idx.store(idx, std::memory_order_release);

      size_t last_idx = producer_idx_.fetch_add(1, std::memory_order_acq_rel);
      if (idx < last_idx)
      {
        // we can improve our next producer idx given we know global last producer idx
        producer_progress_[p.producer_id_].idx.store(last_idx, std::memory_order_release);
      }

      return last_idx;
    }

    template <class Producer>
    size_t aquire_first_idx(Producer& p) requires(_MAX_PRODUCER_N_ == 1)
    {
      // we use CAS here just in case producer has detached in one thread, but than shortly attached in another!
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

  ConsumerContext consumer_ctx_;
  ProducerContext producer_ctx_;

  alignas(_CACHE_PREFETCH_SIZE_) std::atomic<size_t> next_max_consumer_id_;
  alignas(_CACHE_PREFETCH_SIZE_) std::atomic<size_t> next_max_producer_id_;

public:
  using version_type = VersionType;
  using NodeAllocTraits = typename Base::NodeAllocTraits;
  using Node = typename Base::Node;
  using ConsumerTicket = typename Base::ConsumerTicket;
  using ProducerTicket = typename Base::ProducerTicket;
  using type = typename Base::type;
  using Base::Base;

  SPMCMulticastQueueReliableBounded(std::size_t N, const Allocator& alloc = Allocator())
    : Base(N, alloc), next_max_consumer_id_(0), next_max_producer_id_(0)
  {
  }

  size_t get_producer_idx() const
  {
    return producer_ctx_.producer_idx_.load(std::memory_order_acquire);
  }

  // WARNING: shall it be relaxed really?
  size_t get_consumer_next_idx() const
  {
    return consumer_ctx_.consumer_idx_.load(std::memory_order_acquire);
  }

  template <class Consumer>
  size_t acquire_consumer_idx(Consumer& c)
  {
    return consumer_ctx_.acquire_idx(c);
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

  bool no_producers() const
  {
    if (0 == this->next_max_producer_id_.load(std::memory_order_acquire))
    {
      Spinlock::scoped_lock autolock(this->slow_path_guard_);
      return 0 == this->next_max_producer_id_.load(std::memory_order_acquire);
    }

    return false;
  }

  template <class Consumer>
  ConsumerTicket attach_consumer(Consumer& c)
  {
    for (size_t i = 0; i < _MAX_CONSUMER_N_; ++i)
    {
      std::atomic<bool>& locker = this->consumer_ctx_.consumers_registry_.at(i);
      bool is_locked = locker.load(std::memory_order_acquire);
      if (!is_locked)
      {
        if (locker.compare_exchange_strong(is_locked, true, std::memory_order_acq_rel, std::memory_order_relaxed))
        {
          bool producer_need_to_accept = true;
          if (0 == this->next_max_producer_id_.load(std::memory_order_acquire))
          {
            Spinlock::scoped_lock autolock(this->slow_path_guard_);
            if (0 == this->next_max_producer_id_.load(std::memory_order_acquire))
            {
              // if the queue does not have producers attached, then let's assign the next read idx by ourselves!
              if constexpr (_synchronized_consumer_)
              {
                if constexpr (_MAX_CONSUMER_N_ == 1)
                {
                  this->consumer_ctx_.consumers_progress_[0].idx.store(NEXT_CONSUMER_IDX_NEEDED,
                                                                       std::memory_order_release);
                }
                else
                {
                  this->consumer_ctx_.consumers_progress_[i].idx.store(NEXT_CONSUMER_IDX_NEEDED,
                                                                       std::memory_order_release);
                }
              }
              else
              {
                size_t next_consume_idx = this->get_producer_idx();
                if constexpr (_versionless_)
                {
                  this->consumer_ctx_.consumers_progress_.at(i).idx.store(next_consume_idx, std::memory_order_release);
                }
                else
                {
                  if constexpr (_binary_version_)
                  {
                    version_type previous_version;
                    size_t queue_version = next_consume_idx / this->capacity();
                    previous_version = (queue_version & 1) ? version_type{1u} : version_type{};

                    if constexpr (_MAX_CONSUMER_N_ == 1)
                    {
                      this->consumer_ctx_.consumers_progress_[0].idx.store(
                        next_consume_idx, std::memory_order_release);
                      this->consumer_ctx_.consumers_progress_[0].previous_version.store(
                        previous_version, std::memory_order_release);
                    }
                    else
                    {
                      this->consumer_ctx_.consumers_progress_[i].idx.store(
                        next_consume_idx, std::memory_order_release);
                      this->consumer_ctx_.consumers_progress_[i].previous_version.store(
                        previous_version, std::memory_order_release);
                    }
                  }
                  else
                  {
                    static constexpr char error_msg[]{"does not support this producer type"};
                    fail_to_compile<error_msg>();
                  }
                }
              }
              producer_need_to_accept = false;
            }
          }

          if (producer_need_to_accept)
          {
            if constexpr (_MAX_CONSUMER_N_ == 1)
            {
              this->consumer_ctx_.consumers_progress_[0].idx.store(CONSUMER_JOIN_REQUESTED, std::memory_order_release);
            }
            else
            {
              this->consumer_ctx_.consumers_progress_.at(i).idx.store(CONSUMER_JOIN_REQUESTED,
                                                                      std::memory_order_release);
            }
          }

          {
            Spinlock::scoped_lock autolock(this->slow_path_guard_);
            auto latest_max_consumer_id = this->next_max_consumer_id_.load(std::memory_order_relaxed);
            this->next_max_consumer_id_.store(std::max(i + 1, latest_max_consumer_id), std::memory_order_release);
          }

          if (producer_need_to_accept)
          {
            this->consumer_ctx_.consumers_pending_attach_.fetch_add(1, std::memory_order_release);
          }

          // let's wait for producer to consumer our positions from which we
          // shall start safely consuming
          size_t nobody_to_accept_consumer;
          size_t consumer_next_idx;
          version_type previous_version;
          do
          {
            nobody_to_accept_consumer = no_producers();
            if constexpr (_MAX_CONSUMER_N_ == 1)
            {
              consumer_next_idx = this->consumer_ctx_.consumers_progress_[0].idx.load(std::memory_order_acquire);
              previous_version =
                this->consumer_ctx_.consumers_progress_[0].previous_version.load(std::memory_order_relaxed);
            }
            else
            {
              consumer_next_idx =
                this->consumer_ctx_.consumers_progress_.at(i).idx.load(std::memory_order_acquire);
              previous_version =
                this->consumer_ctx_.consumers_progress_.at(i).previous_version.load(std::memory_order_relaxed);
            }
          } while (!nobody_to_accept_consumer && consumer_next_idx >= CONSUMER_JOIN_INPROGRESS);

          if (nobody_to_accept_consumer && producer_need_to_accept)
          {
            detach_consumer(i);
            return {0,
                    0,
                    CONSUMER_IS_WELCOME,
                    this->items_per_batch_,
                    0,
                    0,
                    ConsumerAttachReturnCode::NoProducers};
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
    std::atomic<bool>& locker = this->consumer_ctx_.consumers_registry_.at(consumer_id);
    bool is_locked = locker.load(std::memory_order_acquire);
    if (is_locked)
    {
      if constexpr (_MAX_CONSUMER_N_ == 1)
      {
        this->consumer_ctx_.consumers_progress_[0].idx.store(CONSUMER_IS_WELCOME, std::memory_order_release);
      }
      else
      {
        this->consumer_ctx_.consumers_progress_.at(consumer_id).idx.store(CONSUMER_IS_WELCOME, std::memory_order_release);
      }

      if (locker.compare_exchange_strong(is_locked, false, std::memory_order_release, std::memory_order_relaxed))
      {
        // this->consumers_pending_dettach_.fetch_add(1, std::memory_order_release);
        //  technically even possible that while we unregistered it another
        //  thread, another consumer stole our slot legitimately and we just
        //  removed it from the registry, effectively leaking it...
        //  so unlocked the same consumer on multiple threads is really a bad
        //  idea

        Spinlock::scoped_lock autolock(this->slow_path_guard_);
        auto latest_max_consumer_id = this->next_max_consumer_id_.load(std::memory_order_relaxed);
        this->next_max_consumer_id_.store(std::max(latest_max_consumer_id, consumer_id), std::memory_order_release);
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
    size_t consumer_next_idx;
    if constexpr (_MAX_CONSUMER_N_ == 1)
    {
      consumer_next_idx = this->consumer_ctx_.consumers_progress_[0].idx.load(std::memory_order_acquire);
    }
    else
    {
      consumer_next_idx = this->consumer_ctx_.consumers_progress_[i].idx.load(std::memory_order_acquire);
    }

    if (CONSUMER_JOIN_REQUESTED == consumer_next_idx)
    {
      size_t min_next_producer_idx_local;
      if constexpr (_synchronized_producer_)
      {
        // when new consumer joins it is important to pass it producer idx for which all the smaller indicies are fully published
        // and visible to other cores too, otherwise consumers may  just wrap around the array and consume non-finished item!
        // Array Size=3, versions [0, 1, 1], If consumers were to join at index 1, it would consume index 1, index 2, then
        // warp to consume index 0 again and if the other publisher were still not completed publishing, it would just
        // consume index 0, because after consuming index 2 it would change next expected version to 0!
        min_next_producer_idx_local = PRODUCER_JOINED;
        auto next_max_producer_id = this->next_max_producer_id_.load(std::memory_order_acquire);
        for (size_t i = 0; i < next_max_producer_id; ++i)
        {
          size_t min_next_producer_idx =
            this->producer_ctx_.producer_progress_[i].idx.load(std::memory_order_acquire);
          if (min_next_producer_idx < PRODUCER_JOINED)
          {
            min_next_producer_idx_local = std::min(min_next_producer_idx_local, min_next_producer_idx);
          }
        }
      }
      else
      {
        min_next_producer_idx_local = consumer_from_idx;
      }

      if (min_next_producer_idx_local == consumer_from_idx)
      {
        typename ConsumerContext::ConsumerProgress* consumer_progress;
        if constexpr (_MAX_CONSUMER_N_ == 1)
        {
          consumer_progress = &this->consumer_ctx_.consumers_progress_[0];
        }
        else
        {
          consumer_progress = &this->consumer_ctx_.consumers_progress_[i];
        }

        // new consumer wants a ticket!
        if (consumer_progress->idx.compare_exchange_strong(
              consumer_next_idx, CONSUMER_JOIN_INPROGRESS, std::memory_order_acquire, std::memory_order_relaxed))
        {
          if constexpr (_synchronized_consumer_)
          {
            // consumer_next_idx = consumer_ctx_.acquire_idx(i);
            // synchronized consumers are a bit special in that they don't lock in consume idx right away when joining as
            // that is not required since they don't have to consume all the messages
            consumer_progress->idx.store(NEXT_CONSUMER_IDX_NEEDED, std::memory_order_release);
          }
          else
          {
            consumer_next_idx = consumer_from_idx;
            size_t queue_version = consumer_next_idx / this->capacity();
            version_type previous_version;
            previous_version = (queue_version & 1) ? version_type{1u} : version_type{};
            consumer_progress->previous_version.store(previous_version, std::memory_order_release);
            consumer_progress->idx.store(consumer_next_idx, std::memory_order_release);
          }

          this->consumer_ctx_.consumers_pending_attach_.fetch_sub(1, std::memory_order_release);
        }
      }
    }
    return consumer_next_idx;
  }

  template <class Producer>
  void halt_producer(Producer& p)
  {
    Spinlock::scoped_lock autolock(this->slow_path_guard_);
    std::atomic<bool>& locker = this->producer_ctx_.producer_registry_.at(p.producer_id_);
    bool is_locked = locker.load(std::memory_order_acquire);
    if (is_locked)
    {
      auto& progress = this->producer_ctx_.producer_progress_.at(p.producer_id_);
      progress.halt.store(true, std::memory_order_relaxed);
    }
    else
    {
      throw std::runtime_error("producer does not exist - unable to halt");
    }
  }

  template <class Producer>
  bool is_halted(const Producer& p) const
  {
    // that is only can by called by an attached and live producer so no extra protection is required!
    const auto& progress = this->producer_ctx_.producer_progress_.at(p.producer_id_);
    return progress.halt.load(std::memory_order_relaxed);
  }

  template <class Producer>
  ProducerTicket attach_producer(Producer& p)
  {
    Spinlock::scoped_lock autolock(this->slow_path_guard_);
    for (size_t i = 0; i < _MAX_PRODUCER_N_; ++i)
    {
      std::atomic<bool>& locker = this->producer_ctx_.producer_registry_.at(i);
      bool is_locked = locker.load(std::memory_order_acquire);
      if (!is_locked)
      {
        this->producer_ctx_.producer_progress_.at(i).idx.store(PRODUCER_IS_WELCOME, std::memory_order_release);
        this->producer_ctx_.producer_progress_.at(i).halt.store(false, std::memory_order_release);
        if (locker.compare_exchange_strong(is_locked, true, std::memory_order_release, std::memory_order_relaxed))
        {
          {
            auto latest_max_producer_id = this->next_max_producer_id_.load(std::memory_order_relaxed);
            this->next_max_producer_id_.store(std::max(i + 1, latest_max_producer_id), std::memory_order_release);
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
    Spinlock::scoped_lock autolock(this->slow_path_guard_);
    std::atomic<bool>& locker = this->producer_ctx_.producer_registry_.at(producer_id);
    bool is_locked = locker.load(std::memory_order_acquire);
    if (is_locked)
    {
      if (locker.compare_exchange_strong(is_locked, false, std::memory_order_release, std::memory_order_relaxed))
      {
        // this->consumers_pending_dettach_.fetch_add(1, std::memory_order_release);
        //  technically even possible that while we unregistered it another
        //  thread, another consumer stole our slot legitimately and we just
        //  removed it from the registry, effectively leaking it...
        //  so unlocked the same consumer on multiple threads is really a bad
        //  idea

        auto new_max_producer_id = _MAX_PRODUCER_N_ - 1;
        while (new_max_producer_id >= 0 && new_max_producer_id != std::numeric_limits<size_t>::max())
        {
          if (this->producer_ctx_.producer_progress_.at(new_max_producer_id).idx.load(std::memory_order_relaxed) ==
              PRODUCER_IS_WELCOME)
            --new_max_producer_id;
          else
          {
            break;
          }
        }

        this->next_max_producer_id_.store(new_max_producer_id + 1, std::memory_order_release);
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
  void unlock_min_producer_idx(size_t original_idx, Producer& p) requires(_synchronized_producer_)
  {
    // this is required for producer to commit its min producer idx so that each producer can push items at
    // its own pace! the point to set idx to PRODUCER_JOINED is to not participate in calculating min producer idx!
    this->producer_ctx_.producer_progress_[p.producer_id_].idx.store(PRODUCER_JOINED, std::memory_order_release);
  }

  template <class Producer>
  void unlock_min_producer_idx(size_t original_idx, Producer& p) requires(!_synchronized_producer_)
  {
    this->producer_ctx_.producer_idx_.store(original_idx + 1, std::memory_order_release);
  }

  template <class Producer, class... Args, bool blocking = Producer::blocking_v>
  ProduceReturnCode emplace(size_t original_idx, Producer& producer, Args&&... args)
  {
    size_t min_next_consumer_idx = producer.get_min_next_consumer_idx_cached();
    bool no_active_consumers =
      min_next_consumer_idx == CONSUMER_IS_WELCOME; // TODO: fix for synchronized consumers!

    bool no_free_slot;
    bool slow_consumer = (original_idx - min_next_consumer_idx >= this->n_);
    no_free_slot = no_active_consumers || slow_consumer;

    bool consumers_pending_attach =
      this->consumer_ctx_.consumers_pending_attach_.load(std::memory_order_acquire);

    while (no_free_slot || consumers_pending_attach)
    {
      size_t min_next_consumer_idx_local;
      size_t min_next_producer_idx_local = original_idx;
      if constexpr (_MAX_CONSUMER_N_ > 1)
      {
        min_next_consumer_idx_local = consumer_ctx_.get_next_idx();
        auto next_max_consumer_id = this->next_max_consumer_id_.load(std::memory_order_acquire);
        for (size_t i = 0; i < next_max_consumer_id; ++i)
        {
          size_t consumer_next_idx = try_accept_new_consumer(i, min_next_producer_idx_local);
          if (consumer_next_idx < NEXT_CONSUMER_IDX_NEEDED)
          {
            min_next_consumer_idx_local = std::min(min_next_consumer_idx_local, consumer_next_idx);
          }
        }
      }
      else
      {
        min_next_consumer_idx_local = CONSUMER_IS_WELCOME;
        size_t consumer_next_idx = try_accept_new_consumer(0, min_next_producer_idx_local);
        if (consumer_next_idx < NEXT_CONSUMER_IDX_NEEDED)
        {
          min_next_consumer_idx_local = std::min(min_next_consumer_idx_local, consumer_next_idx);
        }
      }

      producer.cache_min_next_consumer_idx(min_next_consumer_idx_local);
      // no active consumer check is a great safety guard to prevent producers keep overriding nodes
      // and racing with each other as it facilitates transitive happens
      // before relationship between multiple  producers as well!
      // producers also don't need to track absolute version as simple binary state would be enough
      no_active_consumers = min_next_consumer_idx_local ==
        CONSUMER_IS_WELCOME; // TODO: fix this for synchronized consumers!
      slow_consumer = original_idx - min_next_consumer_idx_local >= this->n_;
      no_free_slot = (no_active_consumers || slow_consumer);

      if constexpr (!blocking)
      {
        if (no_active_consumers)
        {
          return ProduceReturnCode::NoConsumers;
        }
      }

      assert(no_active_consumers || original_idx >= min_next_consumer_idx_local);

      if constexpr (!blocking)
      {
        if (slow_consumer)
        {
          return ProduceReturnCode::SlowConsumer;
        }
      }

      consumers_pending_attach = this->consumer_ctx_.consumers_pending_attach_.load(std::memory_order_acquire);
      if (no_free_slot)
      {
        if (producer.is_halted())
          throw ProducerHaltedExp();

        unroll<_CPU_PAUSE_N_>([]() { _mm_pause(); });
      }
    }

    size_t idx = original_idx & this->idx_mask_;
    size_t node_idx;
    if constexpr (_remap_index_)
    {
      node_idx = map_index<_item_per_prefetch_num_, _cache_line_num_>(original_idx) & this->idx_mask_;
    }
    else
    {
      node_idx = idx;
    }

    Node& node = this->nodes_[node_idx];
    version_type version;
    if constexpr (!_versionless_)
    {
      static_assert(_synchronized_producer_);
      if constexpr (_synchronized_consumer_)
      {
        // cannot estimate properly version as consumer can join / detach dynamically...
        version = div_by_power_of_two(original_idx, this->power_of_two_idx_);
      }
      else
      {
        version = ((div_by_power_of_two(original_idx, this->power_of_two_idx_)) & 1u) ? version_type{1u}
                                                                                      : version_type{0};
      }
    }

    void* storage = node.storage_;
    if constexpr (!std::is_trivially_destructible_v<T>)
    {
      if (original_idx > idx)
      {
        NodeAllocTraits::destroy(this->alloc_, static_cast<T*>(storage));
      }
    }

    NodeAllocTraits::construct(this->alloc_, static_cast<T*>(storage), std::forward<Args>(args)...);
    if constexpr (!_versionless_)
    {
      if constexpr (_synchronized_consumer_)
      {
        node.version_.store(1 + version, std::memory_order_release);
      }
      else
      {
        node.version_.store(version ^ version_type{1u}, std::memory_order_release);
      }
    }

    unlock_min_producer_idx(original_idx, producer);

#ifdef _TRACE_STATS_
    ++producer.stats().pub_num;
#endif

    return ProduceReturnCode::Published;
  }

  size_t calc_min_next_producer_idx() const
  {
    size_t min_next_producer_idx_local = get_producer_idx();
    if constexpr (_synchronized_producer_)
    {
      auto next_max_producer_id = this->next_max_producer_id_.load(std::memory_order_acquire);
      for (size_t i = 0; i < next_max_producer_id; ++i)
      {
        size_t min_next_producer_idx =
          this->producer_ctx_.producer_progress_[i].idx.load(std::memory_order_acquire);
        if (min_next_producer_idx < PRODUCER_JOINED)
        {
          min_next_producer_idx_local = std::min(min_next_producer_idx_local, min_next_producer_idx);
        }
      }
    }

    return min_next_producer_idx_local;
  }

  template <class C>
  const T* peek(Node& node, size_t idx, C& consumer) requires(!_versionless_ and _binary_version_)
  {
    bool halt = false;
    VersionType version = node.version_.load(std::memory_order_acquire);
    if (consumer.previous_version_ ^ version)
    {
      if (idx + 1u == this->n_)
      { // need to
        // rollover
        consumer.previous_version_ = version;
      }

      return reinterpret_cast<const T*>(node.storage_);
    }
    else
    {
      if constexpr (C::blocking_v)
      {
        if (consumer.is_halted()) [[unlikely]]
        {
          // for blocking consumers the only return value is a valid pointer so
          // to avoid been stuck in special edge case when producers don't publish
          // we must throw an exception
          throw ConsumerHaltedExp();
        }
      }

      return nullptr;
    }
  }

  template <class C>
  const T* peek(Node& node, auto expected_version, C& consumer) requires(!_versionless_ and !_binary_version_)
  {
    VersionType version = node.version_.load(std::memory_order_acquire);
    if (expected_version != version)
    {
      if constexpr (C::blocking_v)
      {
        if (consumer.is_halted()) [[unlikely]]
        {
          // for blocking consumers the only return value is a valid pointer so
          // to avoid been stuck in special edge case when producers don't publish
          // we must throw an exception
          throw ConsumerHaltedExp();
        }
      }

      return nullptr;
    }

    return reinterpret_cast<const T*>(node.storage_);
  }

  template <class C>
  const T* peek(Node& node, size_t idx, size_t wrapped_idx, C& consumer) requires(_versionless_&& _batch_consumption_enabled_)
  {
    if (consumer.batch_ctx_.batch_buffer_idx_ < C::BatchContext::_batch_buffer_size_)
    {
      return std::launder(reinterpret_cast<T*>(
        &consumer.batch_ctx_.batch_buffer_[(consumer.batch_ctx_.batch_buffer_idx_) * sizeof(Node)]));
    }
    else
    {
      size_t producer_idx = consumer.get_min_next_cached_producer_idx();
      if (producer_idx <= idx)
      {
        // let's try to pull the latest min producer idx
        producer_idx = calc_min_next_producer_idx();
        consumer.set_min_next_cached_producer_idx(producer_idx);
        if (producer_idx <= idx)
        {
          if constexpr (C::blocking_v)
          {
            if (consumer.is_halted()) [[unlikely]]
            {
              // for blocking consumers the only return value is a valid pointer so
              // to avoid been stuck in special edge case when producers don't publish
              // we must throw an exception
              throw ConsumerHaltedExp();
            }
          }

          return nullptr;
        }
      }

      size_t items_ready_num = std::min(producer_idx - idx, this->capacity() - wrapped_idx + 1);
      if (items_ready_num >= C::BatchContext::_batch_buffer_size_)
      {
        std::memcpy(&consumer.batch_ctx_.batch_buffer_, &node,
                    C::BatchContext::_batch_buffer_size_ * sizeof(Node));
        consumer.batch_ctx_.batch_buffer_idx_ = 0;
        return std::launder(reinterpret_cast<T*>(
          &consumer.batch_ctx_.batch_buffer_[(consumer.batch_ctx_.batch_buffer_idx_) * sizeof(Node)]));
      }
      else
      {
        return reinterpret_cast<const T*>(node.storage_);
      }
    }
  }

  template <class C>
  const T* peek(Node& node, size_t idx, size_t wraped_idx,
                C& consumer) requires(_versionless_ && not _batch_consumption_enabled_)
  {
    size_t producer_idx = consumer.get_min_next_cached_producer_idx();
    if (producer_idx <= idx)
    {
      // let's try to pull the latest min producer idx
      producer_idx = calc_min_next_producer_idx();
      consumer.set_min_next_cached_producer_idx(producer_idx);
      if (producer_idx <= idx)
      {
        if constexpr (C::blocking_v)
        {
          if (consumer.is_halted()) [[unlikely]]
          {
            // for blocking consumers the only return value is a valid pointer so
            // to avoid been stuck in special edge case when producers don't publish
            // we must throw an exception
            throw ConsumerHaltedExp();
          }
        }

        return nullptr;
      }
    }

    return reinterpret_cast<const T*>(node.storage_);
  }

  template <class C>
  void skip(size_t idx, size_t& queue_idx, Node& node, auto version,
            C& consumer) requires(!_versionless_ && _binary_version_)
  {
    ++consumer.consumer_next_idx_;
    if (consumer.consumer_next_idx_ == consumer.next_checkout_point_idx_)
    {
      if constexpr (_MAX_CONSUMER_N_ == 1)
      {
        this->consumer_ctx_.consumers_progress_[0].idx.store(consumer.consumer_next_idx_, std::memory_order_release);
      }
      else
      {
        this->consumer_ctx_.consumers_progress_[consumer.consumer_id_].idx.store(
          consumer.consumer_next_idx_, std::memory_order_release);
      }
      consumer.next_checkout_point_idx_ = consumer.consumer_next_idx_ + this->items_per_batch_;
    }
  }

  template <class C>
  void skip(size_t idx, size_t& queue_idx, Node& node, version_type version,
            version_type expected_version, C& consumer) requires(!_versionless_ and !_binary_version_)
  {
    if constexpr (_MAX_CONSUMER_N_ == 1)
    {
      this->consumer_ctx_.consumers_progress_[0].idx.store(consumer.consumer_next_idx_, std::memory_order_release);
    }
    else
    {
      this->consumer_ctx_.consumers_progress_[consumer.consumer_id_].idx.store(
        NEXT_CONSUMER_IDX_NEEDED, std::memory_order_release);
    }

    consumer.consumer_next_idx_ = NEXT_CONSUMER_IDX_NEEDED;
  }

  template <class C>
  void skip(size_t idx, size_t& queue_idx, Node& node, C& consumer) requires(_versionless_ and _synchronized_consumer_)
  {
    if constexpr (_MAX_CONSUMER_N_ == 1)
    {
      this->consumer_ctx_.consumers_progress_[0].idx.store(consumer.consumer_next_idx_, std::memory_order_release);
    }
    else
    {
      this->consumer_ctx_.consumers_progress_[consumer.consumer_id_].idx.store(
        NEXT_CONSUMER_IDX_NEEDED, std::memory_order_release);
    }
    consumer.consumer_next_idx_ = NEXT_CONSUMER_IDX_NEEDED;
  }

  template <class C>
  void skip(size_t idx, size_t& queue_idx, Node& node, C& consumer) requires(_versionless_ and not _synchronized_consumer_)
  {
    if constexpr (_batch_consumption_enabled_)
    {
      if (consumer.batch_ctx_.batch_buffer_idx_ < C::BatchContext::_batch_buffer_size_)
        ++consumer.batch_ctx_.batch_buffer_idx_;
    }

    ++consumer.consumer_next_idx_;
    if (consumer.consumer_next_idx_ == consumer.next_checkout_point_idx_)
    {
      if constexpr (_MAX_CONSUMER_N_ == 1)
      {
        this->consumer_ctx_.consumers_progress_[0].idx.store(consumer.consumer_next_idx_, std::memory_order_release);
      }
      else
      {
        this->consumer_ctx_.consumers_progress_[consumer.consumer_id_].idx.store(
          consumer.consumer_next_idx_, std::memory_order_release);
      }
      consumer.next_checkout_point_idx_ = consumer.consumer_next_idx_ + this->items_per_batch_;
    }
  }
};
