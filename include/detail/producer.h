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
#include <memory>

template <class Queue>
class ProducerBase
{
protected:
  Queue* q_{nullptr};
  size_t producer_idx_{0};
  size_t min_next_consumer_idx_{CONSUMER_IS_WELCOME};
  friend Queue;

public:
  ProducerBase() = default;
  ProducerBase(Queue& q) { attach(q); }
  ~ProducerBase() { detach(); }
  bool attach(Queue& q)
  {
    if (q_)
    {
      return false;
    }

    q_ = std::to_address(&q);
    return true;
  }
  bool detach()
  {
    if (q_)
    {
      q_ = nullptr;
      producer_idx_ = 0;
      min_next_consumer_idx_ = CONSUMER_IS_WELCOME;
      return true;
    }
    else
    {
      return false;
    }
  }
};

template <class Queue>
class alignas(128) ProducerBlocking : public ProducerBase<Queue>
{
public:
  static constexpr bool blocking_v = true;
  using ProducerBase<Queue>::ProducerBase;

  template <class... Args>
  ProducerReturnCode emplace(Args&&... args)
  {
    this->producer_idx_ = this->q_->aquire_idx();
    return this->q_->emplace(*this, std::forward<Args>(args)...);
  }
};

template <class Queue>
class alignas(128) ProducerNonBlocking : public ProducerBase<Queue>
{
public:
  static constexpr bool blocking_v = false;
  using ProducerBase<Queue>::ProducerBase;

  template <class... Args>
  ProducerReturnCode emplace(Args&&... args)
  {
    if (0 == this->producer_idx_)
    {
      this->producer_idx_ = this->q_->aquire_idx();
    } // otherwise the old index has to be re-used

    ProducerReturnCode r = this->q_->emplace(*this, std::forward<Args>(args)...);
    if (r == ProducerReturnCode::Published)
    {
      this->producer_idx_ = 0; // need to refresh the index next time emplace is called!
    }

    return r;
  }
};
