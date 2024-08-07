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
#include "detail/spin_lock.h"
#include <array>
#include <atomic>
#include <cstdlib>
#include <deque>
#include <initializer_list>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <type_traits>
#include <utility>

template <class Queue>
class alignas(64) ConsumerBase
{
protected:
  Queue* q_;
  size_t n_;
  size_t consumer_next_idx_;
  typename Queue::version_type previous_version_;
  size_t idx_mask_;
  size_t consumer_id_;
  size_t next_checkout_point_idx_;
  size_t items_per_batch_;
  mutable size_t queue_idx_;
  std::atomic_bool slow_consumer_; // TODO: not implemented fully yet

  friend Queue;
  friend typename Queue::Base;

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
      if (!q_->detach_consumer(consumer_id_))
      {
        throw DetachConsumerExp(std::string("failed to detach producer [")
                                  .append(std::to_string(this->consumer_id_).append("]"))
                                  .c_str());
      }

      q_ = nullptr;
      return true;
    }
    else
      return false;
  }

  void set_slow_consumer() noexcept { slow_consumer_.store(true, std::memory_order_release); }
  bool is_slow_consumer() const noexcept { return slow_consumer_.load(std::memory_order_acquire); }
  bool is_stopped() const noexcept { return this->q_->is_stopped(); }

  bool empty() const { return this->q_->empty(this->queue_idx_, *this); }

  void increment_queue_idx(size_t queue_sz, size_t batch_sz)
  {
    idx_mask_ = queue_sz - 1;
    items_per_batch_ = batch_sz;
    next_checkout_point_idx_ = items_per_batch_ + (consumer_next_idx_ - consumer_next_idx_ % items_per_batch_);
    n_ = consumer_next_idx_ & idx_mask_; // need to adjust n_!!!!
    if (0 == n_)
      n_ = queue_sz;

    ++queue_idx_;
  }

protected:
  ConsumerAttachReturnCode do_attach(Queue* q)
  {
    if (q_)
    {
      return ConsumerAttachReturnCode::AlreadyAttached;
    }

    auto ticket = q->attach_consumer(*this);
    if (ticket.ret_code == ConsumerAttachReturnCode::Attached)
    {
      n_ = ticket.n;
      idx_mask_ = n_ - 1;

      q_ = q;
      consumer_id_ = ticket.consumer_id;
      consumer_next_idx_ = ticket.consumer_next_idx;
      items_per_batch_ = ticket.items_per_batch;
      queue_idx_ = ticket.queue_idx;

      if (std::numeric_limits<size_t>::max() != ticket.items_per_batch)
      {
        // conflated queue does not use it
        next_checkout_point_idx_ = ticket.items_per_batch +
          (consumer_next_idx_ - ticket.consumer_next_idx % ticket.items_per_batch);
      }

      previous_version_ = ticket.previous_version; // it will be properly recalculated later on by consumers!
    }

    return ticket.ret_code;
  }
};

template <class Consumer>
struct const_consumer_iterator
{
private:
  Consumer* c_;

public:
  using value_type = typename Consumer::T;

  using iterator_category = std::input_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using pointer = const value_type*;
  using reference = const value_type&;

  struct proxy
  {
    value_type v;
    value_type operator*() { return v; }
  };

  const_consumer_iterator() noexcept : c_(nullptr) {}
  const_consumer_iterator(Consumer* c) noexcept : c_(c)
  {
    if (nullptr == c_ || nullptr == c_->peek())
    {
      // nothing to consume, hence we reached the end
      c_ = nullptr;
    }
  }
  const_consumer_iterator(const_consumer_iterator&& other) noexcept
    : const_consumer_iterator(std::move(other).c_)
  {
    other.c_ = nullptr;
  }
  ~const_consumer_iterator() noexcept = default;

  const_consumer_iterator& operator=(const_consumer_iterator&& other) noexcept
  {
    c_ = std::move(other).c_;
    other.c_ = nullptr;
  }

  reference operator*() const
  {
    auto* r = c_->peek();
    if (r == nullptr)
    {
      throw std::runtime_error(
        "item is not available through operator*() "
        "as most likely queue has stopped or consumer became slow! You can query if consumer was "
        "slow by calling is_slow_consumer func on the consumer object");
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
        "as most likely queue has stopped or consumer became slow! You can query if consumer was "
        "slow by calling is_slow_consumer func on the consumer object");
    }
    return r;
  }

  const_consumer_iterator& operator++()
  {
    if constexpr (Consumer::blocking_v)
    {
      if (c_->skip() != ConsumeReturnCode::Consumed || nullptr == c_->peek())
        c_ = nullptr; // reached the end! so effectively it is end iterator now
    }
    else
    {
      if (c_->skip() != ConsumeReturnCode::Consumed || c_->empty())
        c_ = nullptr; // reached the end! so effectively it is end iterator now
    }

    return *this;
  }

  proxy operator++(int) requires(std::is_copy_constructible_v<value_type> ||
                                 std::is_move_constructible_v<value_type>)
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
    if constexpr (Consumer::blocking_v)
    {
      // in blocking mode it is important to do peek after skip since you'd want to block until you've incremented
      // and if increment failed because the queue has stopped than you can just compare the iterator to the end iterator
      if (c_->skip() != ConsumeReturnCode::Consumed || c_->peek() != ConsumeReturnCode::Consumed)
        c_ = nullptr; // reached the end! so effectively it is end iterator now
    }
    else
    {
      if (c_->skip() != ConsumeReturnCode::Consumed || c_->empty())
        c_ = nullptr; // reached the end! so effectively it is end iterator now
    }
    return v;
  }

  friend bool operator==(const const_consumer_iterator& l, const const_consumer_iterator& r)
  {
    return l.c_ == r.c_;
  }
  friend bool operator!=(const const_consumer_iterator& l, const const_consumer_iterator& r)
  {
    return !operator==(l, r);
  }
};
template <class Queue>
struct ConsumerBlocking : ConsumerBase<Queue>
{
  using ConsumerBase<Queue>::ConsumerBase;
  using T = typename Queue::type;
  using const_iterator =
    const_consumer_iterator<ConsumerBlocking<Queue>>; // htypename ConsumerBase<Queue>::template const_iterator<ConsumerBlocking<Queue>>;
  static constexpr bool blocking_v = true;

  const_iterator cbegin() requires std::input_iterator<const_iterator>
  {
    return const_iterator(this);
  }
  const_iterator cend() requires std::input_iterator<const_iterator> { return const_iterator(); }

  const T* peek() const { return this->q_->peek_blocking(this->queue_idx_, *this); }

  template <class F>
  ConsumeReturnCode consume(F&& f) requires(std::is_void_v<decltype((std::forward<F>(f)(std::declval<T>()), void()))>)
  {
    return this->q_->consume_blocking(this->queue_idx_, *this, std::forward<F>(f));
  }

  ConsumeReturnCode skip() { return this->q_->skip_blocking(this->queue_idx_, *this); }

  ConsumeReturnCode consume(T& dst) requires std::is_default_constructible_v<T>
  {
    return this->q_->consume_raw_blocking(this->queue_idx_, reinterpret_cast<void*>(&dst), *this);
  }

  T consume() requires std::is_trivially_copyable_v<T>
  {
    alignas(T) std::byte raw[sizeof(T)];
    auto ret_code = this->q_->consume_raw_blocking(this->queue_idx_, raw, *this);
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
    return this->q_->consume_raw_blocking(this->queue_idx_, dst, *this);
  }
};

template <class Queue>
struct ConsumerNonBlocking : ConsumerBase<Queue>
{
  using ConsumerBase<Queue>::ConsumerBase;
  using T = typename Queue::type;

  static constexpr bool blocking_v = false;
  using const_iterator =
    const_consumer_iterator<ConsumerNonBlocking<Queue>>; // typename ConsumerBase<Queue>::template const_iterator<ConsumerNonBlocking<Queue>>;

  const_iterator cbegin() requires std::input_iterator<const_iterator>
  {
    return const_iterator(this);
  }
  const_iterator cend() requires std::input_iterator<const_iterator> { return const_iterator(); }

  const T* peek() const { return this->q_->peek_non_blocking(this->queue_idx_, *this); }

  template <class F>
  ConsumeReturnCode consume(F&& f) requires(std::is_void_v<decltype((std::forward<F>(f)(std::declval<T>()), void()))>)
  {
    return this->q_->consume_non_blocking(this->queue_idx_, *this, std::forward<F>(f));
  }

  ConsumeReturnCode skip() { return this->q_->skip_non_blocking(this->queue_idx_, *this); }

  ConsumeReturnCode consume(T& dst) requires std::is_default_constructible_v<T>
  {
    return this->q_->consume_raw_non_blocking(this->queue_idx_, reinterpret_cast<void*>(&dst), *this);
  }

  ConsumeReturnCode consume_raw(void* dst) requires std::is_trivially_copyable_v<T>
  {
    return this->q_->consume_raw_non_blocking(this->queue_idx_, dst, *this);
  }
};

// TODO: how to ensure consumer group cannot out-live the queues themsleves?
template <class Queue, size_t MAX_SIZE = 8>
struct alignas(64) AnycastConsumerGroup
{
  using T = typename Queue::type;
  using ConsumerType = ConsumerNonBlocking<Queue>;

  std::array<ConsumerType, MAX_SIZE> consumers;
  std::array<std::atomic<Queue*>, MAX_SIZE> queues;
  std::array<Spinlock, MAX_SIZE> queue_locks;
  std::atomic_size_t max_idx{std::numeric_limits<size_t>::max()};
  Spinlock slow_path_guard_;

  AnycastConsumerGroup() = default;
  AnycastConsumerGroup(std::initializer_list<Queue*> queues)
  {
    for (Queue* q : queues)
    {
      attach(q);
    }
  }

  ~AnycastConsumerGroup()
  {
    for (Queue* q : queues)
    {
      detach(q);
    }
  }

  size_t size() const { return max_idx.load(std::memory_order_acquire) + 1; }

  ConsumerAttachReturnCode attach(Queue* q)
  {
    Spinlock::scoped_lock autolock(slow_path_guard_);
    for (size_t i = 0; i < MAX_SIZE; ++i)
    {
      if (q == queues[i].load(std::memory_order_acquire))
        return ConsumerAttachReturnCode::AlreadyAttached;
    }

    for (size_t i = 0; i < MAX_SIZE; ++i)
    {
      std::unique_lock slot_lock(this->queue_locks[i]);
      Queue* current_queue = queues[i].load(std::memory_order_acquire);
      if (current_queue == nullptr)
      {
        auto r = consumers[i].attach(*q);
        if (r == ConsumerAttachReturnCode::Attached)
        {
          queues[i].store(q, std::memory_order_release);
          slot_lock.unlock();

          size_t current_max_idx = max_idx.load(std::memory_order_relaxed);
          max_idx.store(current_max_idx == std::numeric_limits<size_t>::max() ? i : std::max(i, current_max_idx),
                        std::memory_order_release);
        }
        return r;
      }
    }

    return ConsumerAttachReturnCode::ConsumerLimitReached;
  }

  bool detach(Queue* q)
  {
    Spinlock::scoped_lock autolock(this->slow_path_guard_);
    for (size_t i = 0; i < MAX_SIZE; ++i)
    {
      Spinlock::scoped_lock slot_lock(this->queue_locks[i]);
      bool is_queue_found = this->queues[i].load(std::memory_order_acquire) == q;
      if (is_queue_found)
      {
        consumers[i].detach();
        this->queues[i].store(nullptr, std::memory_order_release);
        auto new_max_queue_id = MAX_SIZE;
        while (new_max_queue_id > 0)
        {
          if (this->queues[new_max_queue_id - 1].load(std::memory_order_acquire) == nullptr)
          {
            --new_max_queue_id;
          }
          else
          {
            break;
          }
        }

        this->max_idx.store(new_max_queue_id - 1 /*has to be prev idx */, std::memory_order_release);
        return true;
      }
    }

    return false;
  }
};

template <class Queue>
class alignas(64) AnycastConsumerBlocking
{
  using ConsumerGroupType = AnycastConsumerGroup<Queue>;
  size_t current_queue_idx_{0};
  AnycastConsumerGroup<Queue>& consumer_group_;

public:
  using T = typename Queue::type;

  AnycastConsumerBlocking(AnycastConsumerGroup<Queue>& consumer_group)
    : consumer_group_(consumer_group)
  {
  }

  template <class F>
  ConsumeReturnCode consume(F&& f) requires(std::is_void_v<decltype((std::forward<F>(f)(std::declval<T>()), void()))>)
  {
    return do_consume_blocking(
      [f_captured = std::forward<F>(f)]<typename ConsumerType>(ConsumerType& consumer_non_blocking) mutable
      { return consumer_non_blocking.consume(std::forward<F>(f_captured)); });
  }

  ConsumeReturnCode consume(T& dst) requires std::is_default_constructible_v<T>
  {
    return do_consume_blocking(
      [&dst]<typename ConsumerType>(ConsumerType& consumer_non_blocking) mutable
      { return consumer_non_blocking.consume_raw(reinterpret_cast<void*>(&dst)); });
  }

  T consume() requires std::is_trivially_copyable_v<T>
  {
    alignas(T) std::byte raw[sizeof(T)];
    auto ret_code = consume_raw(raw);
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
    else
    {
      throw std::runtime_error(std::string("unknown consumer ret code [")
                                 .append(std::to_string(static_cast<int>(ret_code)))
                                 .append("]"));
    }
  }

  ConsumeReturnCode consume_raw(void* dst) requires(std::is_trivially_copyable_v<T>&& std::is_trivially_destructible_v<T>)
  {

    return do_consume_blocking([&dst]<typename ConsumerType>(ConsumerType& consumer_non_blocking)
                               { return consumer_non_blocking.consume_raw(dst); });
  }

private:
  template <class F>
  ConsumeReturnCode do_consume_blocking(F&& consumer_routine) requires(
    std::is_same_v<decltype(std::forward<F>(consumer_routine)(std::declval<std::add_lvalue_reference_t<typename ConsumerGroupType::ConsumerType>>())), ConsumeReturnCode>)
  {
    size_t queues_num;
    uint32_t consecutive_stops = 0;
    ConsumeReturnCode prev_ret = ConsumeReturnCode::NothingToConsume;

    do
    {
      queues_num = consumer_group_.size();
      if (0 == queues_num)
      {
        // if there are no active consumers attached, there is nothing to consumer either and to avoid been stuck we have to return
        return ConsumeReturnCode::NothingToConsume;
      }

      size_t idx = current_queue_idx_;
      Spinlock& spinlock = consumer_group_.queue_locks[idx];
      if (spinlock.try_lock())
      {
        Spinlock::scoped_lock autounlock(spinlock, std::adopt_lock);
        if (consumer_group_.queues[idx].load(std::memory_order_relaxed) != nullptr)
        {
          // this check is very necesary as by the time we've been trying to get a lock the queue might have been detached or even newly attached
          typename ConsumerGroupType::ConsumerType& consumer_non_blocking =
            consumer_group_.consumers[current_queue_idx_];
          ConsumeReturnCode current_ret = std::forward<F>(consumer_routine)(consumer_non_blocking);
          if (current_ret == ConsumeReturnCode::Consumed)
          {
            return current_ret;
          }

          if (ConsumeReturnCode::Stopped == current_ret && (consecutive_stops == 0 || current_ret == prev_ret))
          {
            ++consecutive_stops;
          }
          else
          {
            consecutive_stops = 0;
          }

          prev_ret = current_ret;
        }
      }

      current_queue_idx_ = (current_queue_idx_ + 1) % queues_num; // WARNING! this is slow!
    } while (prev_ret == ConsumeReturnCode::NothingToConsume || queues_num > consecutive_stops);

    return prev_ret;
  }
};

template <class Queue>
class alignas(64) AnycastConsumerNonBlocking
{
  using ConsumerGroupType = AnycastConsumerGroup<Queue>;
  size_t current_queue_idx_{0};
  AnycastConsumerGroup<Queue>& consumer_group_;

public:
  using T = typename Queue::type;

  AnycastConsumerNonBlocking(AnycastConsumerGroup<Queue>& consumer_group)
    : consumer_group_(consumer_group)
  {
  }

  template <class F>
  ConsumeReturnCode consume(F&& f) requires(std::is_void_v<decltype((std::forward<F>(f)(std::declval<T>()), void()))>)
  {
    return do_consume_nonblocking(
      [f_captured = std::forward<F>(f)]<typename ConsumerType>(ConsumerType& consumer_non_blocking) mutable
      { return consumer_non_blocking.consume(std::forward<F>(f_captured)); });
  }

  ConsumeReturnCode consume(T& dst) requires std::is_default_constructible_v<T>
  {
    return do_consume_nonblocking(
      [&dst]<typename ConsumerType>(ConsumerType& consumer_non_blocking) mutable
      { return consumer_non_blocking.consume_raw(reinterpret_cast<void*>(&dst)); });
  }

  T consume() requires std::is_trivially_copyable_v<T>
  {
    alignas(T) std::byte raw[sizeof(T)];
    auto ret_code = consume_raw(raw);
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
    else
    {
      throw std::runtime_error(std::string("unknown consumer ret code [")
                                 .append(std::to_string(static_cast<int>(ret_code)))
                                 .append("]"));
    }
  }

  ConsumeReturnCode consume_raw(void* dst) requires(std::is_trivially_copyable_v<T>&& std::is_trivially_destructible_v<T>)
  {

    return do_consume_nonblocking([&dst]<typename ConsumerType>(ConsumerType& consumer_non_blocking)
                                  { return consumer_non_blocking.consume_raw(dst); });
  }

private:
  template <class F>
  ConsumeReturnCode do_consume_nonblocking(F&& consumer_routine) requires(
    std::is_same_v<decltype(std::forward<F>(consumer_routine)(std::declval<std::add_lvalue_reference_t<typename ConsumerGroupType::ConsumerType>>())), ConsumeReturnCode>)
  {
    uint32_t consecutive_stops = 0;
    size_t idx = current_queue_idx_;
    size_t orig_idx = idx;
    do
    {
      size_t queues_num = consumer_group_.size();
      if (0 == queues_num)
      {
        return ConsumeReturnCode::NothingToConsume;
      }

      current_queue_idx_ = (current_queue_idx_ + 1) % queues_num; // WARNING! this is slow!

      Spinlock& spinlock = consumer_group_.queue_locks[idx];
      if (spinlock.try_lock())
      {
        Spinlock::scoped_lock autounlock(spinlock, std::adopt_lock);
        if (consumer_group_.queues[idx].load(std::memory_order_relaxed) != nullptr)
        {
          // this check is very necesary as by the time we've been trying to get a lock the queue might have been detached or even newly attached
          typename ConsumerGroupType::ConsumerType& consumer_non_blocking = consumer_group_.consumers[idx];
          ConsumeReturnCode r = std::forward<F>(consumer_routine)(consumer_non_blocking);
          if (r != ConsumeReturnCode::NothingToConsume)
            return r;
        }
      }

      idx = current_queue_idx_;
    } while (idx != orig_idx);

    return ConsumeReturnCode::NothingToConsume;
  }
};
