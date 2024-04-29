#pragma once

#include "common.h"

template <class Queue, bool blocking = true>
struct alignas(128) Producer
{
  static constexpr bool blocking_v = blocking;

  Queue& q_;
  size_t producer_idx_{0};
  size_t min_next_consumer_idx_{CONSUMER_IS_WELCOME};

  Producer(Queue& q) : q_(q)
  {
    // the idea is that when a producer gets associated with a queue, we do make sure to start the
    // queue so that users won't need to call start funciton on the queue seprately
    q_.start();
  }

  Producer(Producer&& other) : q_(other.q_), producer_idx_(other.producer_idx_) {}

  template <class... Args>
  ProducerReturnCode emplace(Args&&... args)
  {
    if constexpr (blocking)
    {
      producer_idx_ = q_.aquire_idx();
    }
    else
    {
      if (0 == producer_idx_)
      {
        producer_idx_ = q_.aquire_idx();
      } // otherwise the old index has to be re-used
    }

    ProducerReturnCode r = q_.emplace(*this, std::forward<Args>(args)...);
    if constexpr (!blocking)
    {
      if (r != ProducerReturnCode::Published)
      {
        producer_idx_ = 0; // rest!
      }
    }
    return r;
  }
};
