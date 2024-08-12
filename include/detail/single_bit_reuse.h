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

template <class T>
concept msb_always_0 = requires(T v)
{
  {
    typename T::has_free_bit_0 {}
    } -> std::same_as<std::true_type>;
  {
    v.read_version()
    } -> std::same_as<std::uint8_t>;
  {v.flip_version()};
  {v.release_version()};
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

  void flip_version()
  {
    T tmp = val;
    tmp ^= single_bit_1_masks<T>[MsbIdx<T>];
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
