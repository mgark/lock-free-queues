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

enum class ProducerKind
{
  SingleThreaded,
  Synchronized
};

enum class ProduceReturnCode
{
  Published,
  NotRunning,
  TryAgain
};

enum class ConsumerAttachReturnCode
{
  Stopped,
  ConsumerLimitReached,
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

class QueueStoppedExp : public std::exception
{
};

class SlowConsumerExp : public std::exception
{
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