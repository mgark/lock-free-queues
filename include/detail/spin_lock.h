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

class Spinlock
{
  std::atomic<uint8_t> lock_{0};

public:
  using scoped_lock = std::lock_guard<Spinlock>;

  bool try_lock() noexcept
  {
    if (!lock_.load(std::memory_order_relaxed) && !lock_.exchange(1, std::memory_order_acquire))
      return true;

    return false;
  }

  void lock() noexcept
  {
    while (1)
    {
      if (!lock_.load(std::memory_order_relaxed) && !lock_.exchange(1, std::memory_order_acquire))
        return;
    }
  }

  void unlock() noexcept { lock_.store(0, std::memory_order_release); }
};
