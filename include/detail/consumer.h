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
#include <stdexcept>

template <class Queue>
class alignas(128) ConsumerBase
{
protected:
  Queue* q_;
  size_t n_;
  size_t consumer_next_idx_;
  size_t previous_version_;
  size_t idx_mask_;
  size_t consumer_id_;
  size_t next_checkout_point_idx_;
  size_t items_per_batch_;
  std::atomic_bool slow_consumer_; // TODO: not implemented fully yet

  friend Queue;

public:
  using T = typename Queue::type;

  ConsumerBase() : q_(nullptr), slow_consumer_(false) {}
  ConsumerBase(Queue& q) : q_(nullptr), slow_consumer_(false)
  {
    if (ConsumerAttachReturnCode::Attached != attach(q))
    {
      throw std::runtime_error(
        "could not attach the consumer to the queue - because either there is no space for more "
        "consumers / queue has been stopped");
    }
  }
  ~ConsumerBase() { detach(); }

  ConsumerAttachReturnCode attach(Queue& q) { return do_attach(std::to_address(&q)); }
  bool detach()
  {
    if (q_)
    {
      q_->detach_consumer(consumer_id_);
      q_ = nullptr;
      return true;
    }
    else
      return false;
  }

  void set_slow_consumer() noexcept { slow_consumer_.store(true, std::memory_order_release); }
  bool is_slow_consumer() const noexcept { return slow_consumer_.load(std::memory_order_acquire); }
  bool is_stopped() const noexcept { return this->q_->is_stopped(); }

  template <class Derived>
  struct const_iterator
  {
  private:
    Derived* c_;

  public:
    struct proxy
    {
      T v;
      T operator*() { return v; }
    };

    using iterator_category = std::input_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = const T*;
    using reference = const T&;

    const_iterator() noexcept : c_(nullptr) {}
    const_iterator(Derived* c) noexcept : c_(c) {}
    const_iterator(const_iterator&& other) noexcept : c_(other.c_) { other.c_ = nullptr; }
    ~const_iterator() noexcept = default;

    const_iterator& operator=(const_iterator&& other) noexcept
    {
      c_ = other.c_;
      other.c_ = nullptr;
    }

    reference operator*() const
    {
      auto* r = c_->peek();
      if (r == nullptr)
      {
        throw std::runtime_error(
          "item is not available through operator*() "
          "as most likely queue has stopped");
      }
      return *r;
    }
    pointer operator->() const
    {
      auto* r = c_->peek();
      if (r == nullptr)
      {
        throw std::runtime_error(
          "item is not available through operator*() "
          "as most likely queue has stopped");
      }
      return r;
    }

    const_iterator& operator++()
    {
      if (c_->skip() != ConsumeReturnCode::Consumed)
      {
        c_ = nullptr; // reached the end! so effectively it is end iterator now
      }

      return *this;
    }

    proxy operator++(int) requires(std::is_copy_constructible_v<T> || std::is_move_constructible_v<T>)
    {
      auto* r = c_->peek();
      if (r == nullptr)
      {
        throw std::runtime_error(
          "item is not available through operator*() "
          "as most likely queue has stopped or consumer became slow! You can query if consumer was "
          "slow by calling is_slow_consumer func on the consumer object");
      }

      proxy v(std::move(*r));
      if (!c_->skip())
      {
        c_ = nullptr; // reached the end! so effectively it is end iterator now
      }
      return v;
    }

    friend bool operator==(const const_iterator& l, const const_iterator& r)
    {
      return l.c_ == r.c_;
    }

    friend bool operator!=(const const_iterator& l, const const_iterator& r)
    {
      return !operator==(l, r);
    }
  };

protected:
  ConsumerAttachReturnCode do_attach(Queue* q)
  {
    if (q_)
    {
      return ConsumerAttachReturnCode::AlreadyAttached;
    }

    n_ = q->size();
    idx_mask_ = n_ - 1;

    auto ticket = q->attach_consumer(*this);
    if (ticket.ret_code == ConsumerAttachReturnCode::Attached)
    {
      q_ = q;
      consumer_id_ = ticket.consumer_id;
      consumer_next_idx_ = ticket.consumer_next_idx;
      items_per_batch_ = ticket.items_per_batch;
      if (std::numeric_limits<size_t>::max() != ticket.items_per_batch)
      {
        // conflated queue does not use it
        next_checkout_point_idx_ = ticket.items_per_batch +
          (consumer_next_idx_ - ticket.consumer_next_idx % ticket.items_per_batch);
      }
      previous_version_ = consumer_next_idx_ / n_;
    }

    return ticket.ret_code;
  }
};

template <class Queue>
struct ConsumerBlocking : ConsumerBase<Queue>
{
  using ConsumerBase<Queue>::ConsumerBase;
  using T = typename Queue::type;
  using const_iterator = typename ConsumerBase<Queue>::template const_iterator<ConsumerBlocking<Queue>>;
  static constexpr bool blocking_v = true;

  const_iterator cbegin() requires std::input_iterator<const_iterator>
  {
    return const_iterator(this);
  }
  const_iterator cend() requires std::input_iterator<const_iterator> { return const_iterator(); }

  bool empty() const { return this->q_->empty(this->consumer_next_ids_ & this->idx_mask_, *this); }

  const T* peek() const
  {
    size_t idx = this->consumer_next_idx_ & this->idx_mask_;
    return this->q_->peek_blocking(idx, *this);
  }

  template <class F>
  ConsumeReturnCode consume(F&& f) requires(std::is_void_v<decltype((std::forward<F>(f)(std::declval<T>()), void()))>)
  {
    size_t idx = this->consumer_next_idx_ & this->idx_mask_;
    return this->q_->consume_blocking(idx, *this, std::forward<F>(f));
  }

  ConsumeReturnCode skip()
  {
    size_t idx = this->consumer_next_idx_ & this->idx_mask_;
    return this->q_->skip_blocking(idx, *this);
  }

  ConsumeReturnCode consume(T& dst) requires std::is_default_constructible_v<T>
  {
    size_t idx = this->consumer_next_idx_ & this->idx_mask_;
    return this->q_->consume_raw_blocking(idx, reinterpret_cast<void*>(&dst), *this);
  }

  T consume() requires std::is_trivially_copyable_v<T>
  {
    size_t idx = this->consumer_next_idx_ & this->idx_mask_;
    alignas(T) std::byte raw[sizeof(T)];
    auto ret_code = this->q_->consume_raw_blocking(idx, raw, *this);
    if (ConsumeReturnCode::Consumed == ret_code)
    {
      return *std::launder(reinterpret_cast<T*>(raw));
    }
    else if (ConsumeReturnCode::Stopped == ret_code)
    {
      throw QueueStoppedExp();
    }
    else if (ConsumeReturnCode::SlowConsumer == ret_code)
    {
      throw SlowConsumerExp();
    }

    std::terminate();
  }

  ConsumeReturnCode consume_raw(void* dst) requires std::is_trivially_copyable_v<T>
  {
    size_t idx = this->consumer_next_idx_ & this->idx_mask_;
    return this->q_->consume_raw_blocking(idx, dst, *this);
  }
};

template <class Queue>
struct ConsumerNonBlocking : ConsumerBase<Queue>
{
  using ConsumerBase<Queue>::ConsumerBase;
  using T = typename Queue::type;

  static constexpr bool blocking_v = false;
  using const_iterator = typename ConsumerBase<Queue>::template const_iterator<ConsumerNonBlocking<Queue>>;

  const_iterator cbegin() requires std::input_iterator<const_iterator>
  {
    return const_iterator(this);
  }
  const_iterator cend() requires std::input_iterator<const_iterator> { return const_iterator(); }

  bool empty() const { return this->q_->empty(this->consumer_next_ids_ & this->idx_mask_, *this); }

  const T* peek() const
  {
    size_t idx = this->consumer_next_idx_ & this->idx_mask_;
    return this->q_->peek_non_blocking(idx, *this);
  }

  template <class F>
  ConsumeReturnCode consume(F&& f) requires(std::is_void_v<decltype((std::forward<F>(f)(std::declval<T>()), void()))>)
  {
    size_t idx = this->consumer_next_idx_ & this->idx_mask_;
    return this->q_->consume_non_blocking(idx, *this, std::forward<F>(f));
  }

  ConsumeReturnCode skip()
  {
    size_t idx = this->consumer_next_idx_ & this->idx_mask_;
    return this->q_->skip_non_blocking(idx, *this);
  }

  ConsumeReturnCode consume(T& dst) requires std::is_default_constructible_v<T>
  {
    size_t idx = this->consumer_next_idx_ & this->idx_mask_;
    return this->q_->consume_raw_non_blocking(idx, reinterpret_cast<void*>(&dst), *this);
  }

  ConsumeReturnCode consume_raw(void* dst) requires std::is_trivially_copyable_v<T>
  {
    size_t idx = this->consumer_next_idx_ & this->idx_mask_;
    return this->q_->consume_raw_non_blocking(idx, dst, *this);
  }
};
