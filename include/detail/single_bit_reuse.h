
/*
 * Copyright(c) 2024-present Mykola Garkusha.
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

#include <array>
#include <atomic>
#include <concepts>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <unistd.h>

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

template <class T>
inline constexpr size_t MsbIdx;

template <std::unsigned_integral T>
inline constexpr size_t MsbIdx<T> = 0;

template <std::signed_integral T>
inline constexpr size_t MsbIdx<T> = 1; // two complement is officially stated in the standard as of C++20

template <class T, class Ptr = T*>
concept msb_always_0 = requires(T v)
{
  requires std::atomic<typename T::type>::is_always_lock_free;
  {
    typename T::has_free_bit_0 {}
    } -> std::same_as<std::true_type>;
  {
    v.read_version()
    } -> std::same_as<std::uint8_t>;
  {
    v.store(T{0})
    } -> std::same_as<void>;
};

template <std::integral T>
class integral_msb_always_0
{
  T val;

public:
  using type = T;
  using has_free_bit_0 = std::true_type;

  integral_msb_always_0() : val{} {}
  explicit integral_msb_always_0(T v) : val(v)
  {
    // let's make sure MSB bit is actually set to 0!
    assert(0 == (v & single_bit_1_masks<T>[MsbIdx<T>]));
  }

  integral_msb_always_0& operator=(T other)
  {
    val = other;
    return *this;
  }

  operator T() const
  { // we must ensure MSB bit is kept 0
    T r = val;
    r &= single_bit_0_masks<T>[MsbIdx<T>];
    return r;
  }

  integral_msb_always_0 operator++()
  {
    ++val;
    return *this;
  }

  uint8_t read_version() const
  {
    std::atomic_ref<T> atomic_val(const_cast<T&>(val));
    T tmp = atomic_val.load(std::memory_order_acquire);
    return (tmp & single_bit_1_masks<T>[MsbIdx<T>]) ? uint8_t{1u} : uint8_t{0};
    // return (val & single_1_bit_masks<T>[MsbIdx<T>]) ? uint8_t{1u} : uint8_t{0};
  }

  void store(T new_val)
  {
    std::atomic_ref<T> atomic_val(const_cast<T&>(val));
    T old_val = atomic_val.load(std::memory_order_acquire);

    auto old_version = (old_val & single_bit_1_masks<T>[MsbIdx<T>]) ? uint8_t{1u} : uint8_t{0};
    if (0 == old_version)
    {
      // set new version!
      new_val ^= single_bit_1_masks<T>[MsbIdx<T>];
    }

    atomic_val.store(new_val, std::memory_order_release);
  }
};
