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
#include <unordered_set>

#include "common_test_utils.h"
#include "detail/common.h"
#include "detail/spmc_bounded_conflated_queue.h"

TEST_CASE("SPSC throughput test")
{
  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 1;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = 10000000;
  using Queue = SPMCBoundedQueue<OrderNonTrivial, ProducerKind::Unordered, _MAX_CONSUMERS_>;
  Queue q(_PUBLISHER_QUEUE_SIZE);

  TLOG << "\nSPSC\n";

  size_t from = std::chrono::system_clock::now().time_since_epoch().count();
  std::vector<std::thread> consumers;
  std::vector<size_t> totalVols(_MAX_CONSUMERS_, 0);
  std::atomic_int consumer_joined_num{0};

  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&q, i, &guard, N, &consumer_joined_num, &totalVols]()
      {
        ConsumerBlocking<Queue> c(q);
        ++consumer_joined_num;
        auto begin = std::chrono::system_clock::now();
        size_t n = 0;
        while (n < N)
        {
          c.consume(
            [consumer_id = i, &n, &q, &totalVols](const OrderNonTrivial& r) mutable
            {
              totalVols[consumer_id] += r.vol;
              n = r.id;
            });
        }

        REQUIRE(totalVols[i] > 0);
        std::scoped_lock lock(guard);
        auto end = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
        auto avg_time_ns = (totalVols[i] ? (nanos.count() / totalVols[i]) : 0);
        TLOG << "Consumer [" << i << "] raw time per one item: " << avg_time_ns << "ns"
             << " consumed [" << totalVols[i] << " items \n";
        CHECK(totalVols[i] == N);
        CHECK(avg_time_ns < 40);
      }));
  }

  std::thread producer(
    [&q, &consumer_joined_num, N]()
    {
      while (consumer_joined_num.load() < _MAX_CONSUMERS_)
        ;

      ProducerBlocking<Queue> p(q);
      auto begin = std::chrono::system_clock::now();
      size_t n = 1;
      while (n <= N)
      {
        if (p.emplace(OrderNonTrivial{n, 1U, 100.1, 'B'}) == ProducerReturnCode::Published)
          ++n;
      }
    });

  producer.join();
  for (auto& c : consumers)
  {
    c.join();
  }
}

#if defined(__x86_64__)
TEST_CASE("Conflated SPMC throughput test")
{
  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = 10000000;
  using Queue = SPMCBoundedConflatedQueue<Order, ProducerKind::Unordered, _MAX_CONSUMERS_>;
  Queue q(_PUBLISHER_QUEUE_SIZE);

  std::vector<std::thread> consumers;
  std::vector<size_t> totalVols(_MAX_CONSUMERS_, 0);
  std::atomic_int consumer_joined_num{0};

  size_t from;
  TLOG << "\n  Conflated throughput test\n";
  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&q, i, &guard, N, &consumer_joined_num, &totalVols]()
      {
        ConsumerBlocking<Queue> c(q);
        ++consumer_joined_num;
        auto begin = std::chrono::system_clock::now();
        size_t n = 0;
        size_t items_num = 0;
        while (n < N)
        {
          c.consume(
            [&items_num, consumer_id = i, &n, &q, &totalVols](const Order& r) mutable
            {
              totalVols[consumer_id] += r.vol;
              n = r.id;
              ++items_num;
            });
        }

        REQUIRE(items_num > 0);
        std::scoped_lock lock(guard);
        auto end = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
        auto avg_time_ns = (items_num ? (nanos.count() / items_num) : 0);
        double conflation_ratio = (double)n / items_num;

        TLOG << "Consumer [" << i << "] raw time per one item: " << avg_time_ns << "ns"
             << " consumed [" << items_num << " items, conflation ratio = [" << conflation_ratio << "] \n";
        CHECK(avg_time_ns < 40);
        CHECK(conflation_ratio < 4.0);
      }));
  }

  std::thread producer(
    [&q, &from, &consumer_joined_num, N]()
    {
      while (consumer_joined_num.load() < _MAX_CONSUMERS_)
        ;

      ProducerBlocking<Queue> p(q);
      from = std::chrono::system_clock::now().time_since_epoch().count();
      size_t n = 1;
      while (n <= N)
      {
        if (p.emplace(Order{n, 1U, 100.1, 'B'}) == ProducerReturnCode::Published)
          ++n;
      }
    });

  producer.join();
  for (auto& c : consumers)
  {
    c.join();
  }
}
#endif

TEST_CASE("Unordered SPMC throughput test")
{
  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = 10000000;
  using Queue = SPMCBoundedQueue<OrderNonTrivial, ProducerKind::Unordered, _MAX_CONSUMERS_>;
  Queue q(_PUBLISHER_QUEUE_SIZE);

  size_t from = std::chrono::system_clock::now().time_since_epoch().count();
  std::vector<std::thread> consumers;
  std::vector<size_t> totalVols(_MAX_CONSUMERS_, 0);
  std::atomic_int consumer_joined_num{0};

  TLOG << "\nUnordered SPMC throughput test\n";
  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&q, i, &guard, N, &consumer_joined_num, &totalVols]()
      {
        try
        {
          ConsumerBlocking<Queue> c(q);
          ++consumer_joined_num;
          auto begin = std::chrono::system_clock::now();
          size_t n = 0;
          while (n < N)
          {
            c.consume(
              [consumer_id = i, &n, &q, &totalVols](const OrderNonTrivial& r) mutable
              {
                totalVols[consumer_id] += r.vol;
                n = r.id;
              });
          }

          REQUIRE(totalVols[i] > 0);
          std::scoped_lock lock(guard);
          auto end = std::chrono::system_clock::now();
          auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
          auto avg_time_ns = (totalVols[i] ? (nanos.count() / totalVols[i]) : 0);
          TLOG << "Consumer [" << i << "] raw time per one item: " << avg_time_ns << "ns"
               << " consumed [" << totalVols[i] << " items \n";
          CHECK(totalVols[i] == N);
          CHECK(avg_time_ns < 150);
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception (10)" << e.what();
        }
      }));
  }

  std::thread producer(
    [&q, &consumer_joined_num, N]()
    {
      try
      {
        while (consumer_joined_num.load() < _MAX_CONSUMERS_)
          ;

        ProducerBlocking<Queue> p(q);
        auto begin = std::chrono::system_clock::now();
        size_t n = 1;
        while (n <= N)
        {
          if (p.emplace(OrderNonTrivial{n, 1U, 100.1, 'B'}) == ProducerReturnCode::Published)
            ++n;
        }
      }
      catch (const std::exception& e)
      {
        TLOG << "\n got exception (9)" << e.what();
      }
    });

  producer.join();
  for (auto& c : consumers)
  {
    c.join();
  }
}

TEST_CASE("Unordered MPMC - consumers joining at random times")
{
  using Queue = SPMCBoundedQueue<Order>;
  constexpr size_t _MAX_PUBLISHERS_ = 4;
  constexpr size_t _MAX_CONSUMERS_ = 2;
  constexpr size_t _MSG_PER_CONSUMER_ = 100000000;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = _MSG_PER_CONSUMER_ * _MAX_PUBLISHERS_;

  std::list<Queue> publishersQueues;
  for (size_t i = 0; i < _MAX_PUBLISHERS_; ++i)
  {
    publishersQueues.emplace_back(_PUBLISHER_QUEUE_SIZE);
    publishersQueues.back().start();
  }

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  size_t i = 0;
  std::vector<std::thread> producers;
  for (auto queueIt = publishersQueues.begin(); queueIt != std::end(publishersQueues); ++queueIt, ++i)
  {
    producers.emplace_back(std::thread(
      [producer_id = i, q = std::ref(*queueIt), N]() mutable
      {
        try
        {
          ProducerNonBlocking<Queue> p(q.get());
          for (size_t i = 1; i <= _MSG_PER_CONSUMER_; ++i)
          {
            p.emplace(Order{i, 1U, 100.1, 'A'});
          }

          q.get().stop();
        }
        catch (const std::exception& e)
        {
          std::cout << "\n got exception";
        }
      }));
  }

  // make sure producer joins first!
  std::vector<std::thread> consumers;
  std::vector<size_t> totalMsgConsumed(_MAX_CONSUMERS_, 0);

  for (i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&publishersQueues, i, N, &totalMsgConsumed]()
      {
        try
        {
          // each consumer thread attaches itself to each producer!
          auto queueIt = publishersQueues.begin();
          ConsumerNonBlocking<Queue> per_pub_consumer[_MAX_PUBLISHERS_]{*queueIt, *++queueIt,
                                                                        *++queueIt, *++queueIt};

          // make sure consumers join at diffrent times
          usleep(100000);

          bool done[_MAX_PUBLISHERS_]{};

          auto begin = std::chrono::system_clock::now();
          std::unordered_set<size_t> active_consumers;
          for (size_t i = 0; i < _MAX_PUBLISHERS_; ++i)
          {
            active_consumers.insert(i);
          }

          while (!active_consumers.empty())
          {
            auto it = std::begin(active_consumers);
            while (it != std::end(active_consumers))
            {
              auto consumer_id = *it;
              bool is_consumer_done = false;
              auto r = per_pub_consumer[consumer_id].consume(
                [&, consumer_id](const Order& r) mutable
                {
                  totalMsgConsumed[i] += r.vol;
                  if (r.id >= _MSG_PER_CONSUMER_) // consumed all messages!
                    is_consumer_done = true;
                });
              if (r != ConsumerReturnCode::Consumed)
                is_consumer_done = true;

              if (is_consumer_done)
                it = active_consumers.erase(it);
              else
                ++it;
            }
          }

          auto end = std::chrono::system_clock::now();
          auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
          auto avg_time_ns = (nanos.count() / totalMsgConsumed[i]);

          TLOG << "Consumer [" << i << "] Avg message time: " << avg_time_ns << "ns"
               << " consumed [" << totalMsgConsumed[i] << " items \n";
        }
        catch (const std::exception& e)
        {
          TLOG << "Got exception " << e.what() << "\n";
          CHECK(false);
        }
      }));
  }

  for (auto& p : producers)
    p.join();
  for (auto& c : consumers)
    c.join();
}

TEST_CASE("MPMC Sequential")
{
  constexpr size_t _MAX_PUBLISHERS_ = 4;
  constexpr size_t _MAX_CONSUMERS_ = 4;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t _MSG_PER_CONSUMER_ = 10000000;
  constexpr size_t N = _MSG_PER_CONSUMER_ * _MAX_PUBLISHERS_;

  using Queue = SPMCBoundedQueue<OrderNonTrivial, ProducerKind::Sequential, _MAX_CONSUMERS_>;
  Queue q(_PUBLISHER_QUEUE_SIZE);

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  std::vector<std::thread> consumers;
  std::vector<size_t> totalVols(_MAX_CONSUMERS_, 0);
  std::atomic_int consumer_joined_num{0};

  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&q, i, N, &consumer_joined_num, &totalVols]()
      {
        try
        {
          size_t consumed_msg_num = 0;
          ConsumerBlocking<Queue> consumer(q);
          ++consumer_joined_num;
          auto begin = std::chrono::system_clock::now();
          while (consumed_msg_num < N)
          {
            consumer.consume(
              [i, &consumed_msg_num, &totalVols](const OrderNonTrivial& r) mutable
              {
                totalVols[i] += r.vol;
                if (r.id == _MSG_PER_CONSUMER_)
                  consumed_msg_num += _MSG_PER_CONSUMER_;
              });
          }

          REQUIRE(totalVols[i] > 0);
          auto end = std::chrono::system_clock::now();
          auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
          auto avg_time_ns = nanos.count() / totalVols[i];
          TLOG << "Consumer [" << i << "] raw latency: " << avg_time_ns << "ns"
               << " consumed [" << totalVols[i] << " items \n";
          CHECK(N == totalVols[i]);
          CHECK(avg_time_ns < 300);
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception (4)" << e.what();
        }
      }));
  }

  while (consumer_joined_num < _MAX_CONSUMERS_)
    ;

  size_t i = 0;
  std::vector<std::thread> producers;
  for (; i < _MAX_PUBLISHERS_; ++i)
  {
    producers.emplace_back(std::thread(
      [producer_id = i, q = std::ref(q), N]() mutable
      {
        try
        {
          ProducerBlocking<Queue> p(q.get());
          auto begin = std::chrono::system_clock::now();
          for (size_t j = 1; j <= _MSG_PER_CONSUMER_; ++j)
          {
            if (p.emplace(OrderNonTrivial{j, 1U, 100.34, 'B'}) == ProducerReturnCode::Published)
            {
              //  std::scoped_lock lock(guard);
              //   std::cout << "[" << producer_id << "] published i=" << i <<
              //   "\n";
            }
          }

          REQUIRE(N > 0);
          auto end = std::chrono::system_clock::now();
          auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
          std::cout << "[" << producer_id << "] Produced " << N << " times " << (nanos.count() / N) << "ns\n";
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception (3)" << e.what();
        }
      }));
  }

  for (auto& p : producers)
  {
    p.join();
  }
  for (auto& c : consumers)
  {
    c.join();
  }
}

#if defined(__x86_64__)
TEST_CASE("Conflated MPMC - consumers joining at random times")
{
  using Queue = SPMCBoundedConflatedQueue<Order>;
  constexpr size_t _MAX_PUBLISHERS_ = 4;
  constexpr size_t _MAX_CONSUMERS_ = 2;
  constexpr size_t _MSG_PER_CONSUMER_ = 100000000;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = _MSG_PER_CONSUMER_ * _MAX_PUBLISHERS_;

  std::list<Queue> publishersQueues;
  for (size_t i = 0; i < _MAX_PUBLISHERS_; ++i)
  {
    publishersQueues.emplace_back(_PUBLISHER_QUEUE_SIZE);
    publishersQueues.back().start();
  }

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  size_t i = 0;
  std::vector<std::thread> producers;
  for (auto publisherIt = publishersQueues.begin(); publisherIt != std::end(publishersQueues);
       ++publisherIt, ++i)
  {
    producers.emplace_back(std::thread(
      [producer_id = i, q = std::ref(*publisherIt), N]() mutable
      {
        ProducerNonBlocking<Queue> p(q.get());
        for (size_t i = 1; i <= _MSG_PER_CONSUMER_; ++i)
          p.emplace(Order{i, 1U, 100.1, 'A'});

        q.get().stop();
      }));
  }

  // make sure producer joins first!
  std::vector<std::thread> consumers;
  std::vector<size_t> totalMsgConsumed(_MAX_CONSUMERS_, 0);

  for (i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&publishersQueues, i, N, &totalMsgConsumed]()
      {
        try
        {
          // each consumer thread attaches itself to each producer!
          auto queueIt = publishersQueues.begin();
          ConsumerNonBlocking<Queue> per_pub_consumer[_MAX_PUBLISHERS_]{*queueIt, *++queueIt,
                                                                        *++queueIt, *++queueIt};

          // make sure consumers join at diffrent times
          usleep(100000);

          bool done[_MAX_PUBLISHERS_]{};

          auto begin = std::chrono::system_clock::now();
          std::unordered_set<size_t> active_consumers;
          for (size_t i = 0; i < _MAX_PUBLISHERS_; ++i)
            active_consumers.insert(i);

          while (!active_consumers.empty())
          {
            auto it = std::begin(active_consumers);
            while (it != std::end(active_consumers))
            {
              auto consumer_id = *it;
              bool is_consumer_done = false;
              auto r = per_pub_consumer[consumer_id].consume(
                [&, consumer_id](const Order& r) mutable
                {
                  totalMsgConsumed[i] += r.vol;
                  if (r.id >= _MSG_PER_CONSUMER_) // consumed all messages!
                    is_consumer_done = true;
                });
              if (r != ConsumerReturnCode::Consumed)
                is_consumer_done = true;

              if (is_consumer_done)
                it = active_consumers.erase(it);
              else
                ++it;
            }
          }

          auto end = std::chrono::system_clock::now();
          auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
          auto avg_time_ns = (nanos.count() / totalMsgConsumed[i]);

          TLOG << "Consumer [" << i << "] Avg message time: " << avg_time_ns << "ns"
               << " consumed [" << totalMsgConsumed[i] << " items \n";
        }
        catch (const std::exception& e)
        {
          TLOG << "Got exception " << e.what() << "\n";
          CHECK(false);
        }
      }));
  }

  for (auto& p : producers)
    p.join();
  for (auto& c : consumers)
    c.join();
}
#endif

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }
