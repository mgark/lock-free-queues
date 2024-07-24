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

#include <catch2/catch_all.hpp>
#include <iostream>
#include <list>
#include <mpmc.h>
#include <mutex>
#include <random>
#include <thread>

#include "common_test_utils.h"
#include "detail/common.h"

TEST_CASE("SingleThreaded SPMC attach detach test")
{
  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = 10'000'000'000;
  constexpr size_t ATTACH_DETACH_ITERATIONS = 200;
  constexpr size_t CONSUMER_N = N / ATTACH_DETACH_ITERATIONS / 100;
  using Queue = SPMCMulticastQueueReliableBounded<OrderNonTrivial, 2 * _MAX_CONSUMERS_>;
  Queue q(_PUBLISHER_QUEUE_SIZE);

  size_t from = std::chrono::system_clock::now().time_since_epoch().count();
  std::vector<std::thread> consumers;
  std::atomic_int consumer_joined_num{0};

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&q, i, &guard, CONSUMER_N, &consumer_joined_num]()
      {
        // each consumer would attach / detach themslevs ATTACH_DETACH_ITERATIONS
        for (int j = 0; j < ATTACH_DETACH_ITERATIONS; ++j)
        {
          size_t consumed_num = 0;
          auto begin = std::chrono::system_clock::now();
          ConsumerBlocking<Queue> c(q);
          while (consumed_num < CONSUMER_N)
          {
            c.consume([consumer_id = i, &q, &consumed_num](const OrderNonTrivial& r) mutable
                      { consumed_num += r.vol; });
          }

          std::scoped_lock lock(guard);
          auto end = std::chrono::system_clock::now();
          auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
          auto avg_time_ns = (consumed_num ? (nanos.count() / consumed_num) : 0);
          TLOG << "Consumer [" << i << "] raw time per one item: " << avg_time_ns << "ns"
               << " consumed [" << consumed_num << " items \n";
          CHECK(consumed_num == CONSUMER_N);
        }
        ++consumer_joined_num;
      }));
  }
  std::thread producer(
    [&q, &consumer_joined_num, N]()
    {
      ProducerBlocking<Queue> p(q);
      q.start();

      size_t n = 1;
      while (consumer_joined_num < _MAX_CONSUMERS_)
      {
        auto r = p.emplace(OrderNonTrivial{n, 1U, 100.1, 'B'});
        if (r == ProduceReturnCode::NotRunning)
          break;
        else if (r == ProduceReturnCode::Published)
          n++;
      }

      q.stop();
    });
  for (auto& c : consumers)
  {
    c.join();
  }
  producer.join();
}

TEST_CASE("SingleThreaded ADAPTIVE Blocking SPMC attach detach test")
{
  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = POWER_OF_TWO[22];
  constexpr size_t ATTACH_DETACH_ITERATIONS = 200;
  constexpr size_t CONSUMER_N = N / ATTACH_DETACH_ITERATIONS / 100;
  using Queue = SPMCMulticastQueueReliableAdaptiveBounded<OrderNonTrivial, 2 * _MAX_CONSUMERS_>;
  Queue q(_PUBLISHER_QUEUE_SIZE, N);

  size_t from = std::chrono::system_clock::now().time_since_epoch().count();
  std::vector<std::thread> consumers;
  std::atomic_int consumer_joined_num{0};

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&q, i, &guard, CONSUMER_N, &consumer_joined_num]()
      {
        // each consumer would attach / detach themslevs ATTACH_DETACH_ITERATIONS
        for (int j = 0; j < ATTACH_DETACH_ITERATIONS; ++j)
        {
          size_t consumed_num = 0;
          auto begin = std::chrono::system_clock::now();
          ConsumerBlocking<Queue> c(q);
          while (consumed_num < CONSUMER_N)
          {
            c.consume([consumer_id = i, &q, &consumed_num](const OrderNonTrivial& r) mutable
                      { consumed_num += r.vol; });
          }

          std::scoped_lock lock(guard);
          auto end = std::chrono::system_clock::now();
          auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
          auto avg_time_ns = (consumed_num ? (nanos.count() / consumed_num) : 0);
          TLOG << "Consumer [" << i << "] raw time per one item: " << avg_time_ns << "ns"
               << " consumed [" << consumed_num << " items \n";
          CHECK(consumed_num == CONSUMER_N);
        }
        ++consumer_joined_num;
      }));
  }
  std::thread producer(
    [&q, &consumer_joined_num, N]()
    {
      ProducerBlocking<Queue> p(q);
      q.start();

      size_t n = 1;
      while (consumer_joined_num < _MAX_CONSUMERS_)
      {
        auto r = p.emplace(OrderNonTrivial{n, 1U, 100.1, 'B'});
        if (r == ProduceReturnCode::NotRunning)
          break;
        else if (r == ProduceReturnCode::Published)
          n++;
      }

      std::cout << "\n producer finished n=  " << n << "\n";
      q.stop();
    });
  for (auto& c : consumers)
  {
    c.join();
  }
  producer.join();
}

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }
