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

template <std::integral T, size_t bits_num = sizeof(T) * 8>
std::array<T, bits_num> generate_bit_masks_with_single_1_bit()
{
  std::array<T, bits_num> result;
  for (size_t i = 0; i < bits_num; ++i)
    result[i] = T(1u) << (bits_num - i - 1);

  return result;
}

template <std::integral T, size_t bits_num = sizeof(T) * 8>
std::array<T, bits_num> generate_bit_masks_with_single_0_bit()
{
  std::array<T, bits_num> result = generate_bit_masks_with_single_1_bit<T>();
  for (size_t i = 0; i < bits_num; ++i)
    result[i] = ~result[i];

  return result;
}

template <class T>
inline auto single_0_bit_masks = generate_bit_masks_with_single_0_bit<T>();

template <class T>
inline auto single_1_bit_masks = generate_bit_masks_with_single_1_bit<T>();

template <std::unsigned_integral T>
constexpr size_t MsbIdx = 0;

template <class T>
class IntegralMSBAlways0
{
  T val;

  void set_value(T other)
  {
    auto previous_version = read_version();
    if (0 == previous_version)
    {
      other ^= single_1_bit_masks<T>[MsbIdx<T>];
    }

    std::atomic_ref<T> atomic_val(val);
    atomic_val.store(other, std::memory_order_release);
  }

public:
  using type = T;
  using has_free_0_bit = std::true_type;

  IntegralMSBAlways0() : val(0) {}
  explicit IntegralMSBAlways0(T v) : val(v)
  {
    // let's make sure MSB bit is actually set to 0!
    assert(0 == (v & single_1_bit_masks<T>[MsbIdx<T>]));
  }

  IntegralMSBAlways0(const IntegralMSBAlways0& other) : val(other.val)
  {
    // let's make sure MSB bit is actually set to 0!
  }

  IntegralMSBAlways0(const volatile IntegralMSBAlways0& other) : val(other.val)
  {
    // let's make sure MSB bit is actually set to 0!
  }

  volatile IntegralMSBAlways0& operator=(const IntegralMSBAlways0& other) volatile
  {
    val = other.val;
    return *this;
  }

  IntegralMSBAlways0& operator=(const IntegralMSBAlways0& other)
  {
    val = other.val;
    return *this;
  }

  operator T() const
  { // we must ensure MSB bit is kept 0
    T r = val;
    r &= single_0_bit_masks<T>[MsbIdx<T>];
    return r;
  }

  IntegralMSBAlways0 operator++()
  {
    ++val;
    return *this;
  }

  volatile IntegralMSBAlways0 operator++() volatile
  {
    ++val;
    return *this;
  }

  uint8_t read_version() const
  {
    std::atomic_ref<T> atomic_val(const_cast<T&>(val));
    T tmp = atomic_val.load(std::memory_order_acquire);
    return (tmp & single_1_bit_masks<T>[MsbIdx<T>]) ? uint8_t{1u} : uint8_t{0};
    // return (val & single_1_bit_masks<T>[MsbIdx<T>]) ? uint8_t{1u} : uint8_t{0};
  }

  void flip_version()
  {
    T tmp = val;
    tmp ^= single_1_bit_masks<T>[MsbIdx<T>];
    std::atomic_ref<T> atomic_val(val);
    atomic_val.store(tmp, std::memory_order_release);
    // val ^= single_1_bit_masks<T>[MsbIdx<T>];
  }

  void release_version()
  {
    std::atomic_ref<T> atomic_val(val);
    atomic_val.store(val, std::memory_order_release);
  }
};

/* note that it would generate bit masks by treating each ith bit as if T was stored in the array of bytes,
so you must take into account big /little endian and also actual value interpretation if you need to use it*/
/*template <class T, size_t arr_sz = sizeof(T) * 8>
std::array<T, arr_sz> generate_bit_masks_with_single_0_bit()
{
  std::array<T, arr_sz> result;
  size_t bytes_num = sizeof(T);

  std::byte raw_bytes_all_1bit[bytes_num];
  for (size_t i = 0; i < bytes_num; ++i)
    raw_bytes_all_1bit[i] = ~std::byte{0};

  for (size_t ith_bit = 0; ith_bit < arr_sz; ++ith_bit)
  {
    // need to make sure only ith_bit is set to 0 and the rest kept 1
    std::byte val[bytes_num];
    std::memcpy(&val[0], &raw_bytes_all_1bit[0], bytes_num);

    // get T with all 1 bit set, then just get byte around ith_bit index, convert it to uin8_t and
set the required bit to 0 size_t byte_idx = ith_bit / 8; size_t ith_bit_idx = ith_bit % 8; std::byte
byte_with_one_0 = ~std::byte(uint8_t{1u} << (7 - ith_bit_idx)); val[byte_idx] = byte_with_one_0;

    std::memcpy(&result[ith_bit], &val, bytes_num);
  }

  return result;
}*/

// inline auto INT_MASKS_SINGLE_0_BIT = generate_bit_masks_with_single_0_bit<int>();

//#define _DISABLE_UNRELIABLE_MULTICAST_TEST_
//#define _DISABLE_ADAPTIVE_QUEUE_TEST_
//#define _TRACE_STATS_