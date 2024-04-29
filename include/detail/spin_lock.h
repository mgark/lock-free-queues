#pragma once

#include <atomic>

class Spinlock
{
  std::atomic<uint8_t> lock_{0};

public:
  using scoped_lock = std::lock_guard<Spinlock>;

  void lock() noexcept
  {
    for (;;)
    {
      if (!lock_.load(std::memory_order_relaxed) && !lock_.exchange(1, std::memory_order_acquire))
        return;
    }
  }

  void unlock() noexcept { lock_.store(0, std::memory_order_release); }
};