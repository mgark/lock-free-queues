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
