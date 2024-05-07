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
  { // first increment will make it 0!
    producer_idx_.store(0, std::memory_order_release);
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

  size_t aquire_idx() { return producer_idx_.fetch_add(1, std::memory_order_acquire); }
};

struct alignas(64) ProducerSingleThreadedContext
{
  size_t producer_idx_{std::numeric_limits<size_t>::max()}; // first increment will make it 0!
  size_t min_next_consumer_idx_{CONSUMER_IS_WELCOME};

  size_t get_min_next_consumer_idx_cached() const { return min_next_consumer_idx_; }
  void cache_min_next_consumer_idx(size_t idx) { min_next_consumer_idx_ = idx; }

  size_t aquire_idx() { return ++producer_idx_; }
};

template <class Queue, ProducerKind producerKind, class Derived>
class ProducerBase
{
protected:
  Queue* q_{nullptr};
  friend Queue;

  using ProducerContext =
    std::conditional_t<producerKind == ProducerKind::SingleThreaded, ProducerSingleThreadedContext, ProducerSynchronizedContext&>;

  ProducerContext ctx_;
  size_t last_producer_idx_{0};

public:
  ProducerBase() = default;
  ProducerBase(Queue& q) requires(producerKind == ProducerKind::SingleThreaded) { attach(q); }
  ProducerBase(Queue& q, ProducerSynchronizedContext& ctx) requires(producerKind == ProducerKind::Synchronized)
    : ctx_(ctx)
  {
    attach(q);
  }
  ~ProducerBase() { detach(); }

  bool attach(Queue& q)
  {
    if (q_)
    {
      return false;
    }

    q_ = std::to_address(&q);
    last_producer_idx_ = 0;

    if constexpr (producerKind == ProducerKind::SingleThreaded)
    {
      ctx_.producer_idx_ = std::numeric_limits<size_t>::max();
      ctx_.min_next_consumer_idx_ = CONSUMER_IS_WELCOME;
    }

    return true;
  }

  bool detach()
  {
    if (q_)
    {
      q_ = nullptr;
      return true;
    }
    else
    {
      return false;
    }
  }
  size_t get_min_next_consumer_idx_cached() const
  {
    return ctx_.get_min_next_consumer_idx_cached();
  }

  void cache_min_next_consumer_idx(size_t idx) { ctx_.cache_min_next_consumer_idx(idx); }

  template <class... Args>
  ProduceReturnCode emplace(Args&&... args)
  {
    if (0 == this->last_producer_idx_)
    {
      this->last_producer_idx_ = this->ctx_.aquire_idx();
    }

    auto r = this->q_->emplace(this->last_producer_idx_, *static_cast<Derived*>(this),
                               std::forward<Args>(args)...);
    if (ProduceReturnCode::Published == r)
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
