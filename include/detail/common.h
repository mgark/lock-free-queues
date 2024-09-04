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

#include <atomic>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <pthread.h>
#include <ranges>
#include <stdexcept>
#include <sys/types.h>
#include <type_traits>
#include <unistd.h>

enum class ProducerKind
{
  SingleThreaded,
  Synchronized
};

enum class ProduceReturnCode
{
  Published,
  NotRunning,
  SlowConsumer,
  NoConsumers,
  SlowPublisher
};

enum class ConsumerAttachReturnCode
{
  Stopped,
  ConsumerLimitReached,
  AlreadyAttached,
  Attached
};

enum class ProducerAttachReturnCode
{
  Stopped,
  ProducerLimitReached,
  AlreadyAttached,
  Attached
};

enum class ConsumeReturnCode
{
  NothingToConsume,
  Consumed,
  Stopped,
  SlowConsumer // note that for conflated queues it means that producer warped as consumer was still trying to consume a value
};

enum class QueueState
{
  Created,
  Running,
  Stopped
};

// the order in how numbers are assigned does matter!
constexpr size_t CONSUMER_IS_WELCOME = std::numeric_limits<size_t>::max();
constexpr size_t CONSUMER_JOIN_REQUESTED = CONSUMER_IS_WELCOME - 1;
constexpr size_t CONSUMER_JOIN_INPROGRESS = CONSUMER_JOIN_REQUESTED - 2;
constexpr size_t NEXT_CONSUMER_IDX_NEEDED = CONSUMER_JOIN_INPROGRESS - 3;

constexpr size_t PRODUCER_IS_WELCOME = std::numeric_limits<size_t>::max();
constexpr size_t PRODUCER_JOIN_REQUESTED = PRODUCER_IS_WELCOME - 1;
constexpr size_t PRODUCER_JOIN_INPROGRESS = PRODUCER_JOIN_REQUESTED - 2;
constexpr size_t PRODUCER_JOINED = PRODUCER_JOIN_INPROGRESS - 3;
constexpr size_t NEXT_PRODUCER_IDX_NEEDED = PRODUCER_JOINED - 4;

class QueueStoppedExp : public std::exception
{
};

class SlowConsumerExp : public std::exception
{
};

class DetachConsumerExp : public std::runtime_error
{
public:
  using std::runtime_error::runtime_error;
};

template <size_t N>
constexpr auto calc_power_of_two()
{
  std::array<size_t, N> result{1};
  for (size_t i = 1; i < N; ++i)
    result[i] = result[i - 1] * 2;
  return result;
}

constexpr inline std::array<size_t, 64> POWER_OF_TWO = calc_power_of_two<64>();

template <size_t devisor>
size_t fast_div(size_t dividend)
{
  return dividend / devisor;
}

template <const auto& A, class = std::make_index_sequence<std::tuple_size<std::decay_t<decltype(A)>>::value>>
struct as_sequence;

template <const auto& A, std::size_t... II>
struct as_sequence<A, std::index_sequence<II...>>
{
  using type = std::integer_sequence<typename std::decay_t<decltype(A)>::value_type, A[II]...>;
};

template <size_t... I>
consteval auto do_impl(std::index_sequence<I...>)
{
  return std::array<size_t (*)(size_t dividend), 64>{fast_div<I>...};
}

consteval auto create_power_of_two_dividers() { return do_impl(as_sequence<POWER_OF_TWO>::type()); }
constexpr std::array<size_t (*)(size_t dividend), 64> DIV_BY_POWER_OF_TWO = create_power_of_two_dividers();

size_t div_by_power_of_two(size_t dividend, size_t devisor_idx)
{
  return DIV_BY_POWER_OF_TWO[devisor_idx](dividend);
}

#define _DISABLE_UNRELIABLE_MULTICAST_TEST_
//#define _DISABLE_ADAPTIVE_QUEUE_TEST_
//#define _TRACE_STATS_
//#define _EXTRA_RUNTIME_VERIFICATION_
//#define _TRACE_PRODUCER_IDX_
//#define _ADDITIONAL_TRACE_