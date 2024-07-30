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
#include <atomic>
#include <limits>
#include <memory>

struct alignas(64) ProducerSynchronizedContext
{
  std::atomic<size_t> producer_idx_;
  std::atomic<size_t> min_next_consumer_idx_;

  ProducerSynchronizedContext()
  {
    producer_idx_.store(0, std::memory_order_release);
    min_next_consumer_idx_.store(CONSUMER_IS_WELCOME, std::memory_order_release);
  }

  // size_t min_producer_idx_cached() const { return producer_idx_; }
  size_t get_min_next_consumer_idx_cached() const
  {
    return min_next_consumer_idx_.load(std::memory_order_acquire);
  }

  void cache_min_next_consumer_idx(size_t idx)
  {
    min_next_consumer_idx_.store(idx, std::memory_order_release);
  }

  template <class Producer>
  size_t aquire_idx(Producer& p)
  {
    return producer_idx_.fetch_add(1, std::memory_order_release);
  }
  void release_idx() { producer_idx_.fetch_sub(1, std::memory_order_release); }
};

struct alignas(64) ProducerSingleThreadedContext
{
  size_t producer_idx_{std::numeric_limits<size_t>::max()}; // first increment will make it 0!
  size_t min_next_consumer_idx_{CONSUMER_IS_WELCOME};

  size_t get_min_next_consumer_idx_cached() const { return min_next_consumer_idx_; }
  void cache_min_next_consumer_idx(size_t idx) { min_next_consumer_idx_ = idx; }

  size_t min_producer_idx_cached() const { return producer_idx_; }

  template <class Producer>
  size_t aquire_idx(Producer& p)
  {
    // first increment will make it 0!
    return ++producer_idx_;
  }
};

// TODO: Synchronized context can only work with one queue so we must ensure through interface that is enforced!
template <class Queue, ProducerKind producerKind, class Derived>
class ProducerBase
{
protected:
  Queue* q_{nullptr};
  friend Queue;

  using ProducerContext =
    std::conditional_t<producerKind == ProducerKind::SingleThreaded, ProducerSingleThreadedContext, ProducerSynchronizedContext&>;

  ProducerContext ctx_;
  size_t last_producer_idx_{PRODUCER_IS_WELCOME};
  size_t items_per_batch_;
  size_t next_checkpoint_idx_;
  size_t producer_id_{0};

#ifdef _TRACE_STATS_
  struct Stats
  {
    size_t pub_num;
    size_t cas_num;
  };

  Stats stats_;
#endif

public:
  static constexpr auto producer_kind = producerKind;

#ifdef _TRACE_STATS_
  Stats& stats() { return stats_; }
  const Stats& stats() const { return stats_; }
#endif

  ProducerBase() = default;
  ProducerBase(Queue& q) requires(producerKind == ProducerKind::SingleThreaded)
  {
    if (ProducerAttachReturnCode::Attached != attach(q))
    {
      throw std::runtime_error(
        "could not attach a producer to the queue - because either there is no space for more "
        "consumers / queue has been stopped");
    }
  }
  // TODO: remove this!
  ProducerBase(Queue& q, ProducerSynchronizedContext& ctx) requires(producerKind == ProducerKind::Synchronized)
    : ctx_(ctx)
  {
    if (ProducerAttachReturnCode::Attached != attach(q))
    {
      throw std::runtime_error(
        "could not attach a producer to the queue - because either there is no space for more "
        "consumers / queue has been stopped");
    }
  }
  ~ProducerBase() { detach(); }

  ProducerAttachReturnCode attach(Queue& q)
  {
    if (q_)
    {
      return ProducerAttachReturnCode::AlreadyAttached;
    }

    typename Queue::ProducerTicket ticket = q.attach_producer(*this);
    if (ticket.ret_code == ProducerAttachReturnCode::Attached)
    {
      last_producer_idx_ = PRODUCER_JOIN_INPROGRESS;
      items_per_batch_ = ticket.items_per_batch;
      producer_id_ = ticket.producer_id;
      q_ = &q;
    }

    return ticket.ret_code;
  }

  bool detach()
  {
    if (q_)
    {
      // TODO: need properly implement it as if you were to call it would be crash the program
      if (q_->detach_producer(producer_id_))
      {
        q_ = nullptr;
        return true;
      }
    }

    return false;
  }
  size_t get_min_next_consumer_idx_cached() const
  {
    return ctx_.get_min_next_consumer_idx_cached();
  }

  void cache_min_next_consumer_idx(size_t idx) { ctx_.cache_min_next_consumer_idx(idx); }

  template <class... Args>
  ProduceReturnCode emplace(Args&&... args)
  {
    if (PRODUCER_JOIN_INPROGRESS == this->last_producer_idx_)
    {
      this->last_producer_idx_ = this->q_->aquire_first_idx(static_cast<Derived&>(*this));
      // this->q_->accept_producer(*this);
      next_checkpoint_idx_ =
        items_per_batch_ + (this->last_producer_idx_ - this->last_producer_idx_ % items_per_batch_);
    }
    else if (0 == this->last_producer_idx_)
    {
      this->last_producer_idx_ = this->q_->aquire_idx(static_cast<Derived&>(*this));
    }

    auto r = this->q_->emplace(this->last_producer_idx_, *static_cast<Derived*>(this),
                               std::forward<Args>(args)...);
    if (ProduceReturnCode::Published == r || ProduceReturnCode::SlowPublisher == r)
    {
      this->last_producer_idx_ = 0;
    }

    return r;
  }
};

template <class Queue, ProducerKind producerKind = ProducerKind::SingleThreaded>
class alignas(64) ProducerBlocking
  : public ProducerBase<Queue, producerKind, ProducerBlocking<Queue, producerKind>>
{
public:
  static constexpr bool blocking_v = true;
  using ProducerBase<Queue, producerKind, ProducerBlocking<Queue, producerKind>>::ProducerBase;
};

template <class Queue, ProducerKind producerKind = ProducerKind::SingleThreaded>
class alignas(64) ProducerNonBlocking
  : public ProducerBase<Queue, producerKind, ProducerNonBlocking<Queue, producerKind>>
{
public:
  static constexpr bool blocking_v = false;
  using ProducerBase<Queue, producerKind, ProducerNonBlocking<Queue, producerKind>>::ProducerBase;
};
