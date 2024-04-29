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
  Unordered,
  Sequential
};

enum class ProducerReturnCode
{
  Published,
  NotStarted,
  TryAgain
};

enum class ConsumerReturnCode
{
  NothingToConsume,
  Consumed,
  Stopped,
  SlowConsumer
};

enum class QueueState
{
  CREATED,
  RUNNING,
  STOPPED
};

// the order in how numbers are assigned does matter!
static constexpr size_t CONSUMER_IS_WELCOME = std::numeric_limits<size_t>::max();
static constexpr size_t CONSUMER_JOIN_REQUESTED = CONSUMER_IS_WELCOME - 1;
static constexpr size_t CONSUMER_JOIN_INPROGRESS = CONSUMER_JOIN_REQUESTED - 2;

class QueueStoppedExp : public std::exception
{
};

class SlowConsumerExp : public std::exception
{
};
