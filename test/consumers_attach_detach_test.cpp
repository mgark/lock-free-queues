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

#include <atomic>
#include <catch2/catch_all.hpp>
#include <iostream>
#include <list>
#include <memory>
#include <mpmc.h>
#include <mutex>
#include <random>
#include <thread>

#include "common_test_utils.h"
#include "detail/common.h"
#include "detail/consumer.h"

TEST_CASE("Bounded blocking reliable multicast SPSC attach detach & stress test")
{
  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 1;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = 10'000'000'000;
  constexpr size_t ATTACH_DETACH_ITERATIONS = 20000;
  constexpr size_t CONSUMED_PER_ITERATION = N / ATTACH_DETACH_ITERATIONS / 100;
  using Queue = SPMCMulticastQueueReliableBounded<size_t, _MAX_CONSUMERS_>;
  Queue q(_PUBLISHER_QUEUE_SIZE);

  std::vector<std::thread> consumer_threads;
  std::array<std::atomic<size_t>, _MAX_CONSUMERS_> per_consumer_actual_sum{};
  std::array<std::atomic<size_t>, _MAX_CONSUMERS_> per_consumer_target_sum{};
  std::atomic_int consumer_finished_num{0};

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumer_threads.push_back(std::thread(
      [&q, i, &guard, CONSUMED_PER_ITERATION, &per_consumer_actual_sum, &per_consumer_target_sum, &consumer_finished_num]()
      {
        // each consumer would attach / detach themselves ATTACH_DETACH_ITERATIONS
        for (int j = 0; j < ATTACH_DETACH_ITERATIONS; ++j)
        {
          size_t consumed_num = 0;
          auto begin = std::chrono::system_clock::now();
          ConsumerBlocking<Queue> c(q);
          while (consumed_num < CONSUMED_PER_ITERATION)
          {
            c.consume(
              [consumer_idx = i, &q, CONSUMED_PER_ITERATION, &consumed_num,
               &per_consumer_actual_sum, &per_consumer_target_sum](size_t val) mutable
              {
                uint32_t idx;
                uint32_t producer;
                memcpy(&idx, &val, 4);
                memcpy(&producer, ((char*)&val) + 4, 4);
                per_consumer_actual_sum[consumer_idx].store(
                  per_consumer_actual_sum[consumer_idx].load(std::memory_order_relaxed) + idx,
                  std::memory_order_release);

                if (0 == consumed_num++)
                {
                  // on the very first item, let's calculate our target checksum
                  size_t extra_checksum = 0;
                  for (size_t i = idx; i < idx + CONSUMED_PER_ITERATION; ++i)
                    extra_checksum += i;

                  // relaxed memory order is fine here, because updates are done always by the same thread!
                  per_consumer_target_sum[consumer_idx].store(
                    per_consumer_target_sum[consumer_idx].load(std::memory_order_relaxed) + extra_checksum,
                    std::memory_order_relaxed);
                }
              });
          }

          CHECK(consumed_num == CONSUMED_PER_ITERATION);
        }
        ++consumer_finished_num;
      }));
  }
  std::thread producer(
    [&q, &consumer_finished_num, N]()
    {
      ProducerBlocking<Queue> p(q);
      q.start();

      size_t n = 1;
      while (consumer_finished_num.load(std::memory_order_relaxed) < _MAX_CONSUMERS_)
      {
        auto r = p.emplace_idx();
        if (r == ProduceReturnCode::NotRunning)
          break;
        else if (r == ProduceReturnCode::Published)
          n++;
      }
    });

  for (auto& c : consumer_threads)
    c.join();

  q.stop();
  producer.join();

  size_t consumers_checksum;
  size_t producers_checksum;
  do
  {
    usleep(10000);
    consumers_checksum = 0;
    producers_checksum = 0;
    for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
      consumers_checksum += per_consumer_actual_sum[i].load(std::memory_order_relaxed);
    for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
      producers_checksum += per_consumer_target_sum[i].load(std::memory_order_relaxed);

#ifdef _ADDITIONAL_TRACE_
    TLOG << "\n published items = " << q.get_producer_idx() << " items to consume = " << N
         << " consumers_checksum=" << consumers_checksum << " producers_checksum=" << producers_checksum;
#endif

  } while (consumers_checksum != producers_checksum);
}
TEST_CASE("Bounded blocking reliable multicast SPMC attach detach & stress test")
{
  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = 10'000'000'000;
  constexpr size_t ATTACH_DETACH_ITERATIONS = 20000;
  constexpr size_t CONSUMED_PER_ITERATION = N / ATTACH_DETACH_ITERATIONS / 100;
  using Queue = SPMCMulticastQueueReliableBounded<size_t, 2 * _MAX_CONSUMERS_>;
  Queue q(_PUBLISHER_QUEUE_SIZE);

  std::vector<std::thread> consumer_threads;
  std::array<std::atomic<size_t>, _MAX_CONSUMERS_> per_consumer_actual_sum{};
  std::array<std::atomic<size_t>, _MAX_CONSUMERS_> per_consumer_target_sum{};
  std::atomic_int consumer_finished_num{0};

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumer_threads.push_back(std::thread(
      [&q, i, &guard, CONSUMED_PER_ITERATION, &per_consumer_actual_sum, &per_consumer_target_sum, &consumer_finished_num]()
      {
        // each consumer would attach / detach themselves ATTACH_DETACH_ITERATIONS
        for (int j = 0; j < ATTACH_DETACH_ITERATIONS; ++j)
        {
          size_t consumed_num = 0;
          auto begin = std::chrono::system_clock::now();
          ConsumerBlocking<Queue> c(q);
          while (consumed_num < CONSUMED_PER_ITERATION)
          {
            c.consume(
              [consumer_idx = i, &q, CONSUMED_PER_ITERATION, &consumed_num,
               &per_consumer_actual_sum, &per_consumer_target_sum](size_t val) mutable
              {
                uint32_t idx;
                uint32_t producer;
                memcpy(&idx, &val, 4);
                memcpy(&producer, ((char*)&val) + 4, 4);
                per_consumer_actual_sum[consumer_idx].store(
                  per_consumer_actual_sum[consumer_idx].load(std::memory_order_relaxed) + idx,
                  std::memory_order_release);

                if (0 == consumed_num++)
                {
                  // on the very first item, let's calculate our target checksum
                  size_t extra_checksum = 0;
                  for (size_t i = idx; i < idx + CONSUMED_PER_ITERATION; ++i)
                    extra_checksum += i;

                  // relaxed memory order is fine here, because updates are done always by the same thread!
                  per_consumer_target_sum[consumer_idx].store(
                    per_consumer_target_sum[consumer_idx].load(std::memory_order_relaxed) + extra_checksum,
                    std::memory_order_relaxed);
                }
              });
          }

          CHECK(consumed_num == CONSUMED_PER_ITERATION);
        }
        ++consumer_finished_num;
      }));
  }
  std::thread producer(
    [&q, &consumer_finished_num, N]()
    {
      ProducerBlocking<Queue> p(q);
      q.start();

      size_t n = 1;
      while (consumer_finished_num.load(std::memory_order_relaxed) < _MAX_CONSUMERS_)
      {
        auto r = p.emplace_idx();
        if (r == ProduceReturnCode::NotRunning)
          break;
        else if (r == ProduceReturnCode::Published)
          n++;
      }
    });

  for (auto& c : consumer_threads)
    c.join();

  q.stop();
  producer.join();

  size_t consumers_checksum;
  size_t producers_checksum;
  do
  {
    usleep(10000);
    consumers_checksum = 0;
    producers_checksum = 0;
    for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
      consumers_checksum += per_consumer_actual_sum[i].load(std::memory_order_relaxed);
    for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
      producers_checksum += per_consumer_target_sum[i].load(std::memory_order_relaxed);

#ifdef _ADDITIONAL_TRACE_
    TLOG << "\n published items = " << q.get_producer_idx() << " items to consume = " << N
         << " consumers_checksum=" << consumers_checksum << " producers_checksum=" << producers_checksum;
#endif

  } while (consumers_checksum != producers_checksum);
}

TEST_CASE("Bounded blocking reliable anycast MPMC attach detach & stress test")
{
  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _MAX_PUBLISHERS_ = 4;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 64;
  constexpr size_t _ATTACH_DETACH_ITERATIONS_ = 40000;
  constexpr size_t _N_PER_ITERATION_ = 513;
  constexpr size_t _N_TO_CONSUME_ = _N_PER_ITERATION_ * _ATTACH_DETACH_ITERATIONS_ * _MAX_CONSUMERS_;
  constexpr bool _MULTICAST_ = false;

  using Queue = SPMCMulticastQueueReliableBounded<size_t, _MAX_CONSUMERS_, _MAX_PUBLISHERS_, 4, _MULTICAST_>;

  std::string s;
  std::mutex guard;
  Queue q(_PUBLISHER_QUEUE_SIZE);
  std::srand(std::time(nullptr));

  std::array<std::atomic<size_t>, _MAX_CONSUMERS_> per_consumer_sum{};
  std::vector<std::thread> consumer_threads;
  for (size_t consumer_id = 0; consumer_id < _MAX_CONSUMERS_; ++consumer_id)
  {
    consumer_threads.push_back(std::thread(
      [&, id = consumer_id]()
      {
        try
        {
          for (int i = 0; i < _ATTACH_DETACH_ITERATIONS_; ++i)
          {
            auto c = std::make_unique<ConsumerBlocking<Queue>>(q);
            size_t msg_consumed = 0;
            while (msg_consumed < _N_PER_ITERATION_)
            {
              auto r = c->consume(
                [&](size_t val) mutable
                {
                  uint32_t idx;
                  uint32_t producer;
                  memcpy(&idx, &val, 4);
                  memcpy(&producer, ((char*)&val) + 4, 4);
                  per_consumer_sum[id].store(per_consumer_sum[id].load(std::memory_order_relaxed) + idx,
                                             std::memory_order_release);
                  ++msg_consumed;
                });
            }
          }
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception " << e.what();
        }
      }));
  }

  std::vector<std::thread> producer_threads;
  q.start();

  for (size_t i = 1; i <= _MAX_PUBLISHERS_; ++i)
  {
    producer_threads.emplace_back(std::thread(
      [&q]()
      {
        try
        {
          ProducerBlocking<Queue> p1(q);

          size_t n = 1;
          while (1)
          {
            auto r = p1.emplace_idx();
            if (r == ProduceReturnCode::NotRunning)
              break;
          }
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception " << e.what();
        }
      }));
  }

  for (auto& c : consumer_threads)
    c.join();

  TLOG << "\n all consumers are done\n";
  q.stop();

  for (auto& p : producer_threads)
    p.join();

  TLOG << "\n all producers are done\n";

  size_t producers_checksum = 0;
  for (size_t i = 0; i < _N_TO_CONSUME_; ++i)
    producers_checksum += i;

  size_t consumers_checksum;
  do
  {
    usleep(10000);
    consumers_checksum = 0;
    for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
      consumers_checksum += per_consumer_sum[i].load(std::memory_order_relaxed);
#ifdef _ADDITIONAL_TRACE_
    TLOG << "\n published items = " << q.get_producer_idx() << " items to consume = " << _N_TO_CONSUME_
         << " consumers_checksum=" << consumers_checksum << " producers_checksum=" << producers_checksum;
#endif
  } while (consumers_checksum != producers_checksum);

  CHECK(q.get_producer_idx() - _N_TO_CONSUME_ <= (_PUBLISHER_QUEUE_SIZE + _MAX_PUBLISHERS_));
}

TEST_CASE("Bounded blocking reliable anycast MPSC attach detach & stress test")
{
  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  constexpr size_t _MAX_CONSUMERS_ = 1;
  constexpr size_t _MAX_PUBLISHERS_ = 4;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 64;
  constexpr size_t _ATTACH_DETACH_ITERATIONS_ = 40000;
  constexpr size_t _N_PER_ITERATION_ = 513;
  constexpr size_t _N_TO_CONSUME_ = _N_PER_ITERATION_ * _ATTACH_DETACH_ITERATIONS_ * _MAX_CONSUMERS_;

  using Queue = SPMCMulticastQueueReliableBounded<size_t, _MAX_CONSUMERS_, _MAX_PUBLISHERS_>;
  static_assert(!Queue::_synchronized_consumer_);
  static_assert(Queue::_synchronized_producer_);

  std::string s;
  std::mutex guard;
  Queue q(_PUBLISHER_QUEUE_SIZE);
  std::srand(std::time(nullptr));

  std::vector<std::thread> consumer_threads;
  std::array<std::atomic<size_t>, _MAX_CONSUMERS_> per_consumer_actual_sum{};
  std::array<std::atomic<size_t>, _MAX_CONSUMERS_> per_consumer_target_sum{};

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  std::atomic_bool consumers_done(false);
  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumer_threads.push_back(std::thread(
      [&q, i, &guard, _N_PER_ITERATION_, &per_consumer_actual_sum, &per_consumer_target_sum]()
      {
        // each consumer would attach / detach themselves ATTACH_DETACH_ITERATIONS
        for (int j = 0; j < _N_PER_ITERATION_; ++j)
        {
          size_t consumed_num = 0;
          ConsumerBlocking<Queue> c(q);
          size_t actual_iteration_sum = 0;
          size_t target_iteration_sum = 0;
          size_t starting_idx;
          std::vector<size_t> consumed;
          while (consumed_num < _N_PER_ITERATION_)
          {
            c.consume(
              [consumer_idx = i, &q, _N_PER_ITERATION_, &consumed_num, &target_iteration_sum,
               &actual_iteration_sum, &consumed, &starting_idx, &per_consumer_target_sum](size_t val) mutable
              {
                uint32_t idx;
                uint32_t producer;
                memcpy(&idx, &val, 4);
                memcpy(&producer, ((char*)&val) + 4, 4);
                actual_iteration_sum += idx;
                consumed.push_back(idx);

                if (0 == consumed_num++)
                {
                  // on the very first item, let's calculate our target checksum
                  starting_idx = idx;
                  for (size_t i = idx; i < idx + _N_PER_ITERATION_; ++i)
                    target_iteration_sum += i;
                }
              });
          }

#ifdef _ADDITIONAL_TRACE_
          if (target_iteration_sum != actual_iteration_sum)
            std::abort();
#endif

          // relaxed memory order is fine here, because updates are done always by the same thread!
          per_consumer_actual_sum[i].store(per_consumer_actual_sum[i].load(std::memory_order_relaxed) + actual_iteration_sum,
                                           std::memory_order_release);
          per_consumer_target_sum[i].store(per_consumer_target_sum[i].load(std::memory_order_relaxed) + target_iteration_sum,
                                           std::memory_order_relaxed);
          CHECK(consumed_num == _N_PER_ITERATION_);
        }
      }));
  }
  std::thread producer(
    [&q, &consumers_done]()
    {
      ProducerBlocking<Queue> p(q);
      q.start();

      size_t n = 1;
      while (!consumers_done)
      {
        auto r = p.emplace_idx();
        if (r == ProduceReturnCode::NotRunning)
          break;
        else if (r == ProduceReturnCode::Published)
          n++;
      }

      TLOG << "\n published items = " << n;
    });

  for (auto& c : consumer_threads)
    c.join();

  consumers_done = true;
  q.stop();
  producer.join();

  size_t consumers_checksum;
  size_t producers_checksum;
  do
  {
    usleep(10000);
    consumers_checksum = 0;
    producers_checksum = 0;
    for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
      consumers_checksum += per_consumer_actual_sum[i].load(std::memory_order_relaxed);
    for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
      producers_checksum += per_consumer_target_sum[i].load(std::memory_order_relaxed);

#ifdef _ADDITIONAL_TRACE_
    TLOG << "\n published items = " << q.get_producer_idx() << " items to consume = " << _N_TO_CONSUME_
         << " consumers_checksum=" << consumers_checksum << " producers_checksum=" << producers_checksum;
#endif

  } while (consumers_checksum != producers_checksum);
}

/*TEST_CASE(
  "Bounded blocking reliable synthetic anycast from many queues MPMC attach detach & stress test")
{
  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _MAX_PUBLISHERS_ = 4;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 64;
  constexpr size_t _ATTACH_DETACH_ITERATIONS_ = 40000;
  constexpr size_t _N_PER_ITERATION_ = 513;
  constexpr size_t _N_TO_CONSUME_ = _N_PER_ITERATION_ * _ATTACH_DETACH_ITERATIONS_ * _MAX_CONSUMERS_;
  constexpr bool _MULTICAST_ = true;

  using Queue = SPMCMulticastQueueReliableBounded<size_t, _MAX_CONSUMERS_, _MAX_PUBLISHERS_, 4, _MULTICAST_>;

  std::string s;
  std::mutex guard;
  std::deque<Queue> queues;
  for (size_t i = 0; i < _MAX_PUBLISHERS_; ++i)
    queues.emplace_back(_PUBLISHER_QUEUE_SIZE);

  AnycastConsumerGroup<Queue> consumer_group({std::to_address(&queues.front())});
  std::vector<std::thread> consumers;
  std::srand(std::time(nullptr));

  std::array<std::atomic<size_t>, _MAX_CONSUMERS_> per_consumer_sum{};
  std::vector<std::thread> consumer_threads;
  for (size_t consumer_id = 0; consumer_id < _MAX_CONSUMERS_; ++consumer_id)
  {
    consumer_threads.push_back(std::thread(
      [&, id = consumer_id]()
      {
        try
        {
          AnycastConsumerBlocking<Queue> c(consumer_group);
          for (int i = 0; i < _ATTACH_DETACH_ITERATIONS_; ++i)
          {
            size_t idx = 1 + (std::rand() % (_MAX_PUBLISHERS_ - 1));
            bool attach = (i + id) & 1;
            if (id != 0) // there always must be one consumer alive to guarantee we'd consume all items
            {
              if (attach)
              {
                consumer_group.detach(std::to_address(&queues[idx]));
                consumer_group.attach(std::to_address(&queues[idx]));
              }
              else
              {
                consumer_group.detach(std::to_address(&queues[idx]));
              }
            }

            size_t msg_consumed = 0;
            while (msg_consumed < _N_PER_ITERATION_)
            {
              auto r = c.consume(
                [&](size_t val) mutable
                {
                  uint32_t idx;
                  uint32_t producer;
                  memcpy(&idx, &val, 4);
                  memcpy(&producer, ((char*)&val) + 4, 4);
                  per_consumer_sum[id].store(per_consumer_sum[id].load(std::memory_order_relaxed) + idx,
                                             std::memory_order_release);
                  ++msg_consumed;
                });
            }
          }
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception " << e.what();
        }
      }));
  }

  std::vector<std::thread> producer_threads;
  for (auto queue_it = begin(queues); queue_it != end(queues); ++queue_it)
  {
    producer_threads.emplace_back(std::thread(
      [&q = *queue_it]()
      {
        try
        {
          ProducerBlocking<Queue> p(q);
          q.start();

          size_t n = 1;
          while (1)
          {
            auto r = p.emplace_idx();
            if (r == ProduceReturnCode::NotRunning)
              break;
          }
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception " << e.what();
        }
      }));
  }

  for (auto& c : consumer_threads)
    c.join();

  TLOG << "\n all consumers are done\n";
  for (auto& q : queues)
    q.stop();

  for (auto& p : producer_threads)
    p.join();

  TLOG << "\n all producers are done\n";

  size_t producers_checksum = 0;
  for (size_t i = 0; i < _N_TO_CONSUME_; ++i)
    producers_checksum += i;

  size_t published_num = 0;
  for (auto& queue : queues)
    published_num += queue.get_producer_idx();

  size_t consumers_checksum;
  do
  {
    usleep(10000);
    consumers_checksum = 0;
    for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
      consumers_checksum += per_consumer_sum[i].load(std::memory_order_relaxed);
#ifdef _ADDITIONAL_TRACE_
    TLOG << "\n published items = " << published_num << " items to consume = " << _N_TO_CONSUME_
         << " consumers_checksum=" << consumers_checksum << " producers_checksum=" << producers_checksum;
#endif
  } while (consumers_checksum != producers_checksum);

  // CHECK(q.get_producer_idx() - _N_TO_CONSUME_ <= (_PUBLISHER_QUEUE_SIZE + _MAX_PUBLISHERS_));
}*/

#ifndef _DISABLE_ADAPTIVE_QUEUE_TEST_
TEST_CASE("Adaptive blocking reliable multicast SPMC attach detach & stress test")
{
  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = POWER_OF_TWO[22];
  constexpr size_t ATTACH_DETACH_ITERATIONS = 200;
  constexpr size_t CONSUME_PER_ITERATION = N / ATTACH_DETACH_ITERATIONS;
  using Queue = SPMCMulticastQueueReliableAdaptiveBounded<size_t, 2 * _MAX_CONSUMERS_>;
  Queue q(_PUBLISHER_QUEUE_SIZE, N);

  std::vector<std::thread> consumer_threads;
  std::array<std::atomic<size_t>, _MAX_CONSUMERS_> per_consumer_actual_sum{};
  std::array<std::atomic<size_t>, _MAX_CONSUMERS_> per_consumer_target_sum{};

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  std::atomic_bool consumers_done(false);
  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumer_threads.push_back(std::thread(
      [&q, i, &guard, CONSUME_PER_ITERATION, &per_consumer_actual_sum, &per_consumer_target_sum]()
      {
        // each consumer would attach / detach themselves ATTACH_DETACH_ITERATIONS
        for (int j = 0; j < ATTACH_DETACH_ITERATIONS; ++j)
        {
          size_t consumed_num = 0;
          ConsumerBlocking<Queue> c(q);
          size_t actual_iteration_sum = 0;
          size_t target_iteration_sum = 0;
          size_t starting_idx;
          std::vector<size_t> consumed;
          while (consumed_num < CONSUME_PER_ITERATION)
          {
            c.consume(
              [consumer_idx = i, &q, CONSUME_PER_ITERATION, &consumed_num, &target_iteration_sum,
               &actual_iteration_sum, &consumed, &starting_idx, &per_consumer_target_sum](size_t val) mutable
              {
                uint32_t idx;
                uint32_t producer;
                memcpy(&idx, &val, 4);
                memcpy(&producer, ((char*)&val) + 4, 4);
                actual_iteration_sum += idx;
                consumed.push_back(idx);

                if (0 == consumed_num++)
                {
                  // on the very first item, let's calculate our target checksum
                  starting_idx = idx;
                  for (size_t i = idx; i < idx + CONSUME_PER_ITERATION; ++i)
                    target_iteration_sum += i;
                }
              });
          }

  #ifdef _ADDITIONAL_TRACE_
          if (target_iteration_sum != actual_iteration_sum)
            std::abort();
  #endif

          // relaxed memory order is fine here, because updates are done always by the same thread!
          per_consumer_actual_sum[i].store(per_consumer_actual_sum[i].load(std::memory_order_relaxed) + actual_iteration_sum,
                                           std::memory_order_release);
          per_consumer_target_sum[i].store(per_consumer_target_sum[i].load(std::memory_order_relaxed) + target_iteration_sum,
                                           std::memory_order_relaxed);
          CHECK(consumed_num == CONSUME_PER_ITERATION);
        }
      }));
  }
  std::thread producer(
    [&q, &consumers_done, N]()
    {
      ProducerBlocking<Queue> p(q);
      q.start();

      size_t n = 1;
      while (!consumers_done)
      {
        auto r = p.emplace_idx();
        if (r == ProduceReturnCode::NotRunning)
          break;
        else if (r == ProduceReturnCode::Published)
          n++;
      }

      TLOG << "\n published items = " << n;
    });

  for (auto& c : consumer_threads)
    c.join();

  consumers_done = true;
  q.stop();
  producer.join();

  size_t consumers_checksum;
  size_t producers_checksum;
  do
  {
    usleep(10000);
    consumers_checksum = 0;
    producers_checksum = 0;
    for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
      consumers_checksum += per_consumer_actual_sum[i].load(std::memory_order_relaxed);
    for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
      producers_checksum += per_consumer_target_sum[i].load(std::memory_order_relaxed);

  #ifdef _ADDITIONAL_TRACE_
    TLOG << "\n published items = " << q.get_producer_idx() << " items to consume = " << N
         << " consumers_checksum=" << consumers_checksum << " producers_checksum=" << producers_checksum;
  #endif

  } while (consumers_checksum != producers_checksum);
}
#endif

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }
