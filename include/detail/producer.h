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

template <class Queue, class Derived>
class ProducerBase
{
protected:
  Queue* q_{nullptr};
  friend Queue;

  size_t last_producer_idx_{PRODUCER_IS_WELCOME};
  size_t items_per_batch_;
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
#ifdef _TRACE_STATS_
  Stats& stats() { return stats_; }
  const Stats& stats() const { return stats_; }
#endif

  ProducerBase() = default;
  ProducerBase(Queue& q)
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

  template <class... Args>
  ProduceReturnCode emplace(Args&&... args)
  {
    if (PRODUCER_JOIN_INPROGRESS == this->last_producer_idx_)
    {
      this->last_producer_idx_ = this->q_->aquire_first_idx(static_cast<Derived&>(*this));
    }
    else if (NEXT_PRODUCER_IDX_NEEDED == this->last_producer_idx_)
    {
      this->last_producer_idx_ = this->q_->aquire_idx(static_cast<Derived&>(*this));
    }

    auto r = this->q_->emplace(this->last_producer_idx_, *static_cast<Derived*>(this),
                               std::forward<Args>(args)...);
    if (ProduceReturnCode::Published == r || ProduceReturnCode::SlowPublisher == r)
    {
      this->last_producer_idx_ = NEXT_PRODUCER_IDX_NEEDED;
    }

    return r;
  }
};

template <class Queue>
class alignas(64) ProducerBlocking : public ProducerBase<Queue, ProducerBlocking<Queue>>
{
public:
  static constexpr bool blocking_v = true;
  using ProducerBase<Queue, ProducerBlocking<Queue>>::ProducerBase;
};

template <class Queue>
class alignas(64) ProducerNonBlocking : public ProducerBase<Queue, ProducerNonBlocking<Queue>>
{
public:
  static constexpr bool blocking_v = false;
  using ProducerBase<Queue, ProducerNonBlocking<Queue>>::ProducerBase;
};
