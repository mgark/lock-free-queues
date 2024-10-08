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
#include "detail/spin_lock.h"
#include "spmc_queue_base.h"
#include "spmc_queue_base_adaptive.h"
#include <atomic>
#include <cstdlib>
#include <math.h>
#include <memory>

#ifndef _DISABLE_ADAPTIVE_QUEUE_TEST_

template <class T, size_t _MAX_CONSUMER_N_ = 8, size_t _BATCH_NUM_ = 4, class Allocator = std::allocator<T>, class VersionType = size_t>
class SPMCMulticastQueueReliableAdaptiveBounded
  : public SPMCMulticastQueueAdaptiveBase<T, SPMCMulticastQueueReliableAdaptiveBounded<T, _MAX_CONSUMER_N_, _BATCH_NUM_, Allocator>,
                                          _MAX_CONSUMER_N_, _BATCH_NUM_, Allocator, VersionType>
{
  static constexpr size_t _MAX_PRODUCER_N_ = 1;

  struct alignas(_CACHE_PREFETCH_SIZE_) ConsumerProgress
  {
    std::atomic<size_t> idx;
    std::atomic<size_t> previous_version;
    std::atomic<size_t> queue_idx;
  };

  struct ProducerContext
  {
    alignas(_CACHE_PREFETCH_SIZE_) std::atomic<size_t> producer_idx_;
    alignas(_CACHE_PREFETCH_SIZE_) std::atomic<size_t> min_next_consumer_idx_;
    alignas(_CACHE_PREFETCH_SIZE_) std::atomic<size_t> min_next_producer_idx_;

    struct alignas(_CACHE_PREFETCH_SIZE_) ProducerProgress
    {
      std::atomic<size_t> idx;
    };

    std::array<std::atomic<bool>, _MAX_PRODUCER_N_ + 1> producer_registry_;

    SPMCMulticastQueueReliableAdaptiveBounded& host_;
    ProducerContext(SPMCMulticastQueueReliableAdaptiveBounded& host) : host_(host)
    {
      std::fill(std::begin(producer_registry_), std::end(producer_registry_), 0 /*unlocked*/);

      producer_idx_.store(std::numeric_limits<size_t>::max(), std::memory_order_release);
      min_next_consumer_idx_.store(CONSUMER_IS_WELCOME, std::memory_order_release);
    }

    size_t get_min_next_consumer_idx_cached() const
    {
      return min_next_consumer_idx_.load(std::memory_order_acquire);
    }

    void cache_min_next_consumer_idx(size_t idx)
    {
      min_next_consumer_idx_.store(idx, std::memory_order_release);
    }

    template <class Producer>
    size_t aquire_idx(Producer& p) requires(_MAX_PRODUCER_N_ == 1)
    {
      size_t new_idx = 1 + producer_idx_.load(std::memory_order_acquire);
      producer_idx_.store(new_idx, std::memory_order_release);
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
  };

  ProducerContext producer_ctx_;

  using ConsumerProgressArray = std::array<ConsumerProgress, _MAX_CONSUMER_N_ + 1>;
  using ConsumerRegistryArray = std::array<std::atomic<bool>, _MAX_CONSUMER_N_ + 1>;

  ConsumerProgressArray consumers_progress_;
  ConsumerRegistryArray consumers_registry_;

  // these variables change somewhat infrequently
  alignas(_CACHE_PREFETCH_SIZE_) std::atomic<int> consumers_pending_attach_;
  std::atomic<size_t> max_consumer_id_;
  size_t prev_version_;

  std::vector<std::atomic_size_t> rollover_ids_;

public:
  using version_type = VersionType;
  using Base =
    SPMCMulticastQueueAdaptiveBase<T, SPMCMulticastQueueReliableAdaptiveBounded, _MAX_CONSUMER_N_, _BATCH_NUM_, Allocator, VersionType>;
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

  SPMCMulticastQueueReliableAdaptiveBounded(std::size_t initial_sz, std::size_t max_size,
                                            const Allocator& alloc = Allocator())
    : Base(initial_sz, max_size, alloc),
      producer_ctx_(*this),
      consumers_pending_attach_(0),
      max_consumer_id_(0),
      prev_version_(0),
      rollover_ids_(this->max_queue_num_)
  {
    for (auto it = std::begin(consumers_progress_); it != std::end(consumers_progress_); ++it)
      it->idx.store(CONSUMER_IS_WELCOME, std::memory_order_release);

    // for (auto it = std::begin(rollover_ids_); it != std::end(rollover_ids_); ++it)
    //   it->store(0, std::memory_order_release);

    std::fill(std::begin(consumers_registry_), std::end(consumers_registry_), 0 /*unlocked*/);
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
              auto queue_idx = this->back_buffer_idx_.load(std::memory_order_acquire);
              this->consumers_progress_.at(i).idx.store(0, std::memory_order_release);
              this->consumers_progress_.at(i).previous_version.store(0, std::memory_order_release);
              this->consumers_progress_.at(i).queue_idx.store(queue_idx, std::memory_order_release);
              producer_need_to_accept = false;
            }
          }

          if (producer_need_to_accept)
          {
            this->consumers_progress_.at(i).idx.store(CONSUMER_JOIN_REQUESTED, std::memory_order_release);
          }

          {
            Spinlock::scoped_lock autolock(this->slow_path_guard_);
            auto latest_max_consumer_id = this->max_consumer_id_.load(std::memory_order_relaxed);
            this->max_consumer_id_.store(std::max(i, latest_max_consumer_id), std::memory_order_release);
          }

          if (producer_need_to_accept)
          {
            this->consumers_pending_attach_.fetch_add(1, std::memory_order_release);
          }

          // let's wait for producer to consumer our positions from which we
          // shall start safely consuming
          size_t stopped;
          size_t consumer_next_idx;
          size_t consumer_queue_idx;
          version_type previous_version;
          do
          {
            stopped = is_stopped();
            consumer_next_idx = this->consumers_progress_.at(i).idx.load(std::memory_order_acquire);
            previous_version = this->consumers_progress_.at(i).previous_version.load(std::memory_order_relaxed);
            consumer_queue_idx = this->consumers_progress_.at(i).queue_idx.load(std::memory_order_relaxed);
          } while (!stopped && consumer_next_idx >= CONSUMER_JOIN_INPROGRESS);

          if (stopped)
          {
            detach_consumer(i);
            return {0, 0, CONSUMER_IS_WELCOME, 0 /*nodes per batch does not matter*/, 0, 0, 0, ConsumerAttachReturnCode::NoProducers};
          }

          size_t queue_sz = this->initial_sz_ * POWER_OF_TWO[consumer_queue_idx];
          size_t items_per_batch = queue_sz / _BATCH_NUM_;
          size_t idx_mask = queue_sz - 1;
          size_t version_checkpoint_idx = this->rollover_ids_[consumer_queue_idx] & idx_mask;
          if (version_checkpoint_idx == 0)
            version_checkpoint_idx = queue_sz;
          return {version_checkpoint_idx,
                  i,
                  consumer_next_idx,
                  items_per_batch,
                  consumer_queue_idx,
                  previous_version,
                  idx_mask,
                  ConsumerAttachReturnCode::Attached};
        } // else someone stole the locker just before us!
      }
    }

    // not enough space for the new consumer!
    return {0, 0, CONSUMER_IS_WELCOME, 0 /*nodes per batch does not matter*/, 0, 0, 0, ConsumerAttachReturnCode::ConsumerLimitReached};
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
      if (locker.compare_exchange_strong(is_locked, false, std::memory_order_release, std::memory_order_relaxed))
      {
        // this->consumers_pending_dettach_.fetch_add(1, std::memory_order_release);
        //  technically even possible that while we unregistered it another
        //  thread, another consumer stole our slot legitimately and we just
        //  removed it from the registry, effectively leaking it...
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

  size_t try_accept_new_consumer(size_t i, size_t original_idx, size_t previous_version)
  {
    size_t consumer_next_idx = this->consumers_progress_[i].idx.load(std::memory_order_relaxed);
    if (CONSUMER_JOIN_REQUESTED == consumer_next_idx)
    {
      // new consumer wants a ticket!
      if (this->consumers_progress_[i].idx.compare_exchange_strong(
            consumer_next_idx, CONSUMER_JOIN_INPROGRESS, std::memory_order_relaxed, std::memory_order_relaxed))
      {
        consumer_next_idx = original_idx;
        size_t queue_idx = this->back_buffer_idx_.load(std::memory_order_acquire);
        this->consumers_progress_[i].previous_version.store(previous_version, std::memory_order_relaxed); // WARNING! this would only work for single threaded publishers!
        this->consumers_progress_[i].queue_idx.store(queue_idx, std::memory_order_relaxed);
        this->consumers_progress_[i].idx.store(consumer_next_idx, std::memory_order_release);
        this->consumers_pending_attach_.fetch_sub(1, std::memory_order_release);
      }
    }
    return consumer_next_idx;
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

  size_t get_producer_idx() const
  {
    return producer_ctx_.producer_idx_.load(std::memory_order_relaxed);
  }

  template <class Producer, class... Args, bool blocking = Producer::blocking_v>
  ProduceReturnCode emplace(size_t original_idx, Producer& producer, Args&&... args)
  {

    int i = 0;
    size_t idx = original_idx & this->idx_mask_;
    size_t min_next_consumer_idx = producer_ctx_.get_min_next_consumer_idx_cached();
    size_t back_buffer_idx = this->back_buffer_idx_.load(std::memory_order_acquire);
    bool first_time_publish = min_next_consumer_idx == CONSUMER_IS_WELCOME;
    Node& current_node = this->nodes_[back_buffer_idx][idx];
    size_t version = current_node.version_.load(std::memory_order_acquire); // be careful not gonna work with synchronized producers!
    bool free_node = prev_version_ == version;
    bool no_free_slot = first_time_publish ||
      (original_idx - min_next_consumer_idx >= this->current_sz_ && !free_node);
    while (no_free_slot || this->consumers_pending_attach_.load(std::memory_order_acquire))
    {
      size_t min_next_consumer_idx_local = CONSUMER_IS_WELCOME;
      auto max_consumer_id = this->max_consumer_id_.load(std::memory_order_acquire);
      for (size_t i = 1; i <= max_consumer_id; ++i)
      {
        size_t consumer_next_idx = try_accept_new_consumer(i, original_idx, version);
        if (consumer_next_idx < CONSUMER_JOIN_INPROGRESS)
        {
          min_next_consumer_idx_local = std::min(min_next_consumer_idx_local, consumer_next_idx);
        }
      }

      producer_ctx_.cache_min_next_consumer_idx(min_next_consumer_idx_local);
      first_time_publish = min_next_consumer_idx == CONSUMER_IS_WELCOME;
      no_free_slot = !first_time_publish &&
        (original_idx - min_next_consumer_idx_local >= this->current_sz_) && !free_node;

      if (no_free_slot)
      {
        // let's add another queue to the list with twice the size!
        // back_buffer_idx = this->back_buffer_idx_.load(std::memory_order_acquire);
        if (back_buffer_idx + 1 >= this->max_queue_num_)
        {
          if constexpr (!blocking)
          {
            return ProduceReturnCode::SlowConsumer;
          }
        }
        else
        {
          size_t new_sz = this->current_sz_ * 2;
          this->nodes_[++back_buffer_idx] =
            std::allocator_traits<typename Base::NodeAllocator>::allocate(this->alloc_, new_sz);
          std::uninitialized_value_construct_n(this->nodes_[back_buffer_idx], new_sz);

          // now it is super important to continue previous version in the new queue
          prev_version_ = version;
          for (Node* node = this->nodes_[back_buffer_idx]; node != this->nodes_[back_buffer_idx] + new_sz; ++node)
            node->version_ = prev_version_;

          this->increment_queue_size(new_sz); // now we make new producer queue visible to the consumers!
          idx = original_idx & this->idx_mask_; // index needs to be re-adjusted as the mask changed!
          this->rollover_ids_[back_buffer_idx].store(original_idx, std::memory_order_release);
          break;
        }
      }
    }

    Node& node = this->nodes_[back_buffer_idx][idx];
    void* storage = node.storage_;
    if constexpr (!std::is_trivially_destructible_v<T>)
    {
      if (version > 0)
      {
        NodeAllocTraits::destroy(this->alloc_, static_cast<T*>(storage));
      }
    }

    NodeAllocTraits::construct(this->alloc_, static_cast<T*>(storage), std::forward<Args>(args)...);
    node.version_.store(version + 1, std::memory_order_release);
    return ProduceReturnCode::Published;
  }

  template <class C, class F>
  ConsumeReturnCode consume_by_func(size_t idx, size_t& queue_idx, Node& node, size_t version,
                                    C& consumer, F&& f)
  {
    if (consumer.previous_version_ < version)
    {
      std::forward<F>(f)(node.storage_);
      if (idx + 1 == consumer.n_)
      { // need to
        // rollover
        consumer.previous_version_ = version;
      }

      ++consumer.consumer_next_idx_;
      if (consumer.consumer_next_idx_ == consumer.next_checkout_point_idx_)
      {
        this->consumers_progress_[consumer.consumer_id_].idx.store(consumer.consumer_next_idx_,
                                                                   std::memory_order_release);
        consumer.next_checkout_point_idx_ = consumer.consumer_next_idx_ + consumer.items_per_batch_;
      }

      return ConsumeReturnCode::Consumed;
    }
    else
    {
      size_t last_queue_idx = this->back_buffer_idx_.load(std::memory_order_acquire);
      if (last_queue_idx > queue_idx)
      {
        size_t next_queue_sz = this->initial_sz_ * POWER_OF_TWO[queue_idx + 1];
        size_t next_batch_sz = next_queue_sz / _BATCH_NUM_;
        consumer.increment_queue_idx(next_queue_sz, next_batch_sz);
      }

      return ConsumeReturnCode::NothingToConsume;
    }
  }

  template <class C>
  ConsumeReturnCode skip(size_t idx, size_t& queue_idx, Node& node, size_t version, C& consumer)
  {
    if (consumer.previous_version_ < version)
    {
      if (idx + 1 == consumer.n_)
      { // need to
        // rollover
        consumer.previous_version_ = version;
      }

      ++consumer.consumer_next_idx_;
      if (consumer.consumer_next_idx_ == consumer.next_checkout_point_idx_)
      {
        this->consumers_progress_[consumer.consumer_id_].idx.store(consumer.consumer_next_idx_,
                                                                   std::memory_order_release);
        consumer.next_checkout_point_idx_ = consumer.consumer_next_idx_ + consumer.items_per_batch_;
      }
      return ConsumeReturnCode::Consumed;
    }
    else
    {
      size_t last_queue_idx = this->back_queue_idx.load(std::memory_order_acquire);
      if (last_queue_idx > queue_idx)
      {
        size_t next_queue_sz = this->initial_sz_ * POWER_OF_TWO[queue_idx + 1];
        size_t next_batch_sz = next_queue_sz / _BATCH_NUM_;
        consumer.increment_queue_idx(next_queue_sz, next_batch_sz);
      }

      return ConsumeReturnCode::NothingToConsume;
    }
  }
};
#endif