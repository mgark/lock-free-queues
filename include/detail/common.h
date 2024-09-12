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
#include <bits/utility.h>
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

enum class ProduceReturnCode
{
  Published,
  SlowConsumer,
  NoConsumers,
  SlowPublisher
};

enum class ConsumerAttachReturnCode
{
  NoProducers,
  ConsumerLimitReached,
  AlreadyAttached,
  Attached
};

enum class ProducerAttachReturnCode
{
  ProducerLimitReached,
  AlreadyAttached,
  Attached
};

enum class ConsumeReturnCode
{
  NothingToConsume,
  Consumed,
  SlowConsumer // note that for conflated queues it means that producer warped as consumer was still trying to consume a value
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

template <const char[]>
struct fail_to_compile;

struct VoidType
{
};

class QueueStoppedExp : public std::exception
{
};

class SlowConsumerExp : public std::exception
{
};

class ConsumerHaltedExp : public std::exception
{
};

class ProducerHaltedExp : public std::exception
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

template <class F, size_t... N>
void do_unroll(F&& f, std::index_sequence<N...>)
{
  size_t unused[] = {(std::forward<F>(f)(), N)...};
  (void)unused;
}

template <size_t N, class F>
void unroll(F&& f)
{
  if (N > 0)
  {
    return do_unroll(std::forward<F>(f), std::make_index_sequence<N>{});
  }
}

consteval size_t power_of_2_far(size_t val)
{
  auto far_val = std::lower_bound(begin(POWER_OF_TWO), end(POWER_OF_TWO), val);
  if (far_val == end(POWER_OF_TWO))
  {
    return val;
  }
  else
  {
    return *far_val;
  }
}

// compilers will optimize it given so many constants here
template <size_t ItemsPerCacheLine, size_t CacheLinesNum>
size_t map_index(size_t original_idx)
{
  if constexpr (ItemsPerCacheLine == 0)
    return original_idx;

  constexpr size_t window_size = ItemsPerCacheLine * CacheLinesNum;
  size_t idx = original_idx % window_size;
  size_t window_cache_line_idx = idx % CacheLinesNum;
  size_t cache_line_idx = idx / CacheLinesNum;
  return original_idx - idx + window_cache_line_idx * ItemsPerCacheLine + cache_line_idx;
}

template <std::integral T, size_t bits_num = sizeof(T) * 8>
std::array<T, bits_num> generate_bit_masks_with_single_bit_1()
{
  std::array<T, bits_num> result;
  for (size_t i = 0; i < bits_num; ++i)
    result[i] = T(1u) << (bits_num - i - 1);

  return result;
}

template <std::integral T, size_t bits_num = sizeof(T) * 8>
std::array<T, bits_num> generate_bit_masks_with_single_bit_0()
{
  std::array<T, bits_num> result = generate_bit_masks_with_single_bit_1<T>();
  for (size_t i = 0; i < bits_num; ++i)
    result[i] = ~result[i];

  return result;
}

template <class T>
inline auto single_bit_0_masks = generate_bit_masks_with_single_bit_0<T>();

template <class T>
inline auto single_bit_1_masks = generate_bit_masks_with_single_bit_1<T>();

static constexpr size_t _CACHE_LINE_SIZE_ = 64;
static constexpr size_t _CACHE_PREFETCH_SIZE_ = 128;
static constexpr size_t _MEMCPY_OPTIMAL_BYTES_ = 256;

#define _DISABLE_UNRELIABLE_MULTICAST_TEST_
#define _DISABLE_SYNTHETIC_ANYCAST_TEST_
#define _DISABLE_ADAPTIVE_QUEUE_TEST_
//#define _TRACE_STATS_
//#define _EXTRA_RUNTIME_VERIFICATION_
//#define _TRACE_PRODUCER_IDX_
//#define _ADDITIONAL_TRACE_