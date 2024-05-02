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

template <class Queue>
struct alignas(128) ProducerBlocking
{
  Queue& q_;
  size_t producer_idx_{0};
  size_t min_next_consumer_idx_{CONSUMER_IS_WELCOME};

  static constexpr bool blocking_v = true;

  ProducerBlocking(Queue& q) : q_(q)
  {
    // the idea is that when a producer gets associated with a queue, we do make sure to start the
    // queue so that users won't need to call start funciton on the queue seprately
    q_.start();
  }

  ProducerBlocking(ProducerBlocking&& other) : q_(other.q_), producer_idx_(other.producer_idx_) {}

  template <class... Args>
  ProducerReturnCode emplace(Args&&... args)
  {
    producer_idx_ = q_.aquire_idx();
    return q_.emplace(*this, std::forward<Args>(args)...);
  }
};

template <class Queue>
struct alignas(128) ProducerNonBlocking
{
  Queue& q_;
  size_t producer_idx_{0};
  size_t min_next_consumer_idx_{CONSUMER_IS_WELCOME};

  static constexpr bool blocking_v = false;

  ProducerNonBlocking(Queue& q) : q_(q)
  {
    // the idea is that when a producer gets associated with a queue, we do make sure to start the
    // queue so that users won't need to call start funciton on the queue seprately
    q_.start();
  }

  ProducerNonBlocking(ProducerNonBlocking&& other)
    : q_(other.q_), producer_idx_(other.producer_idx_)
  {
  }

  template <class... Args>
  ProducerReturnCode emplace(Args&&... args)
  {
    if (0 == producer_idx_)
    {
      producer_idx_ = q_.aquire_idx();
    } // otherwise the old index has to be re-used

    ProducerReturnCode r = q_.emplace(*this, std::forward<Args>(args)...);
    if (r == ProducerReturnCode::Published)
      producer_idx_ = 0; // need to refresh the index next time emplace is called!

    return r;
  }
};
