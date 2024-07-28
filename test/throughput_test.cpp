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
#include <mpmc.h>
#include <mutex>
#include <numeric>
#include <random>
#include <thread>
#include <unordered_set>

#include "common_test_utils.h"
#include "detail/common.h"
#include "detail/consumer.h"
#include "detail/producer.h"

TEST_CASE("SPSC throughput test")
{
  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 1;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = 10000000;
  using Queue = SPMCMulticastQueueReliableBounded<OrderNonTrivial, _MAX_CONSUMERS_>;
  Queue q(_PUBLISHER_QUEUE_SIZE);

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  size_t from = std::chrono::system_clock::now().time_since_epoch().count();
  std::vector<std::thread> consumers;
  std::vector<size_t> totalMsgCount(_MAX_CONSUMERS_, 0);
  std::atomic_int consumer_joined_num{0};

  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&q, i, &guard, N, &consumer_joined_num, &totalMsgCount]()
      {
        ConsumerBlocking<Queue> c(q);
        ++consumer_joined_num;
        auto begin = std::chrono::system_clock::now();
        size_t n = 0;
        while (n < N)
        {
          c.consume(
            [consumer_id = i, &n, &q, &totalMsgCount](const OrderNonTrivial& r) mutable
            {
              totalMsgCount[consumer_id] += r.vol;
              n = r.id;
            });
        }

        REQUIRE(totalMsgCount[i] > 0);
        std::scoped_lock lock(guard);
        auto end = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
        auto avg_time_ns = (totalMsgCount[i] ? (nanos.count() / totalMsgCount[i]) : 0);
        TLOG << "Consumer [" << i << "] raw time per one item: " << avg_time_ns
             << "ns, total_time_ms=" << nanos / (1000 * 1000) << " consumed [" << totalMsgCount[i]
             << " items \n";
        CHECK(totalMsgCount[i] == N);
        CHECK(avg_time_ns < 40);
      }));
  }

  while (consumer_joined_num.load() < _MAX_CONSUMERS_)
    ;
  std::thread producer(
    [&q, &consumer_joined_num, N]()
    {
      ProducerBlocking<Queue> p(q);
      q.start();

      auto begin = std::chrono::system_clock::now();
      size_t n = 1;
      while (n <= N)
      {
        if (p.emplace(OrderNonTrivial{n, 1U, 100.1, 'B'}) == ProduceReturnCode::Published)
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
  using Queue = SPMCMulticastQueueUnreliable<Order, _MAX_CONSUMERS_>;
  Queue q(_PUBLISHER_QUEUE_SIZE);

  std::vector<std::thread> consumers;
  std::vector<size_t> totalMsgCount(_MAX_CONSUMERS_, 0);
  std::atomic_int consumer_joined_num{0};

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  size_t from;
  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&q, i, &guard, N, &consumer_joined_num, &totalMsgCount]()
      {
        ConsumerBlocking<Queue> c(q);
        ++consumer_joined_num;
        auto begin = std::chrono::system_clock::now();
        size_t n = 0;
        size_t items_num = 0;
        while (n < N)
        {
          c.consume(
            [&items_num, consumer_id = i, &n, &q, &totalMsgCount](const Order& r) mutable
            {
              totalMsgCount[consumer_id] += r.vol;
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
             << "total_time_ms=" << nanos / (1000 * 1000) << " consumed [" << items_num
             << " items, conflation ratio = [" << conflation_ratio << "] \n";
        CHECK(avg_time_ns < 40);
        CHECK(conflation_ratio < 6.0);
      }));
  }

  while (consumer_joined_num.load() < _MAX_CONSUMERS_)
    ;

  std::thread producer(
    [&q, &from, &consumer_joined_num, N]()
    {
      ProducerBlocking<Queue> p(q);
      q.start();
      from = std::chrono::system_clock::now().time_since_epoch().count();
      size_t n = 1;
      while (n <= N)
      {
        if (p.emplace(Order{n, 1U, 100.1, 'B'}) == ProduceReturnCode::Published)
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

TEST_CASE("Synchronized MPMC throughput test")
{
  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _MAX_PUBLISHERS_ = 5;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 4;
  constexpr size_t _MSG_PER_CONSUMER_ = 1000000;
  constexpr size_t N = _MAX_PUBLISHERS_ * _MSG_PER_CONSUMER_;

  using Queue = SPMCMulticastQueueReliableBounded<OrderNonTrivial, _MAX_CONSUMERS_>;
  Queue queue(_PUBLISHER_QUEUE_SIZE);

  size_t from = std::chrono::system_clock::now().time_since_epoch().count();
  std::vector<std::thread> consumers;
  std::atomic_int consumer_joined_num{0};

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";
  for (size_t consumer_id = 0; consumer_id < _MAX_CONSUMERS_; ++consumer_id)
  {
    consumers.push_back(std::thread(
      [&queue, consumer_id, &guard, N, _MSG_PER_CONSUMER_, &consumer_joined_num]()
      {
        try
        {
          ConsumerBlocking<Queue> c(queue);
          ++consumer_joined_num;
          auto begin = std::chrono::system_clock::now();
          size_t totalMsgConsumed = 0;
          bool is_consumer_done = false;
          while (!is_consumer_done)
          {
            auto r = c.consume(
              [&](const OrderNonTrivial& r) mutable
              {
                totalMsgConsumed += r.vol;
                if (totalMsgConsumed >= N) // consumed all messages!
                  is_consumer_done = true;
              });
            if (r == ConsumeReturnCode::Stopped)
              is_consumer_done = true;
          }

          REQUIRE(totalMsgConsumed > 0);
          std::scoped_lock lock(guard);
          auto end = std::chrono::system_clock::now();
          auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
          auto avg_time_ns = (totalMsgConsumed ? (nanos.count() / totalMsgConsumed) : 0);
          TLOG << "Consumer [" << consumer_id << "] raw time per one item: " << avg_time_ns << "ns"
               << "total_time_ms=" << nanos / (1000 * 1000) << " consumed [" << totalMsgConsumed
               << " items \n";
          CHECK(totalMsgConsumed == N);
          CHECK(avg_time_ns < 2000);
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception " << e.what();
        }
      }));
  }

  while (consumer_joined_num.load() < _MAX_CONSUMERS_)
    ;

  ProducerSynchronizedContext producer_group;
  std::vector<std::thread> producers;
  std::atomic_int publishers_completed_num{0};
  for (size_t producer_id = 0; producer_id < _MAX_PUBLISHERS_; ++producer_id)
  {
    producers.emplace_back(std::thread(
      [&queue, &publishers_completed_num, &producer_group, &consumer_joined_num, N]()
      {
        try
        {
          ProducerBlocking<Queue, ProducerKind::Synchronized> p(queue, producer_group);
          queue.start();

          auto begin = std::chrono::system_clock::now();
          size_t n = 1;
          while (n <= _MSG_PER_CONSUMER_)
          {
            if (p.emplace(OrderNonTrivial{n, 1U, 100.1, 'B'}) == ProduceReturnCode::Published)
              ++n;
          }

          ++publishers_completed_num;
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception " << e.what();
        }
      }));
  }

  while (publishers_completed_num < _MAX_PUBLISHERS_)
    usleep(100000);

  queue.stop();

  for (auto& p : producers)
    p.join();

  for (auto& c : consumers)
    c.join();
}

TEST_CASE("SingleThreaded MPMC throughput test")
{
  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _MAX_PUBLISHERS_ = 4;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t _MSG_PER_CONSUMER_ = 10000000;
  constexpr size_t N = _MAX_PUBLISHERS_ * _MSG_PER_CONSUMER_;

  using Queue = SPMCMulticastQueueReliableBounded<OrderNonTrivial, _MAX_CONSUMERS_>;
  std::list<Queue> queues;
  for (size_t i = 0; i < _MAX_PUBLISHERS_; ++i)
    queues.emplace_back(_PUBLISHER_QUEUE_SIZE);

  size_t from = std::chrono::system_clock::now().time_since_epoch().count();
  std::vector<std::thread> consumers;
  std::atomic_int consumer_joined_num{0};

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";
  for (size_t consumer_id = 0; consumer_id < _MAX_CONSUMERS_; ++consumer_id)
  {
    consumers.push_back(std::thread(
      [&queues, consumer_id, &guard, N, &consumer_joined_num]()
      {
        try
        {
          auto queueIt = queues.begin();
          ConsumerBlocking<Queue> per_pub_consumer[_MAX_PUBLISHERS_]{*queueIt, *++queueIt, *++queueIt, *++queueIt};
          ++consumer_joined_num;
          auto begin = std::chrono::system_clock::now();
          size_t totalMsgConsumed = 0;
          bool is_consumer_done[_MAX_PUBLISHERS_]{};
          while (std::count(std::begin(is_consumer_done), std::end(is_consumer_done), true) < _MAX_PUBLISHERS_)
          {
            for (size_t publisher_id = 0; publisher_id < _MAX_PUBLISHERS_; ++publisher_id)
            {
              auto r = per_pub_consumer[publisher_id].consume(
                [&](const OrderNonTrivial& r) mutable
                {
                  totalMsgConsumed += r.vol;
                  if (r.id >= _MSG_PER_CONSUMER_) // consumed all messages!
                    is_consumer_done[publisher_id] = true;
                });
              if (r == ConsumeReturnCode::Stopped)
                is_consumer_done[publisher_id] = true;
            }
          }

          REQUIRE(totalMsgConsumed > 0);
          std::scoped_lock lock(guard);
          auto end = std::chrono::system_clock::now();
          auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
          auto avg_time_ns = (totalMsgConsumed ? (nanos.count() / totalMsgConsumed) : 0);
          TLOG << "Consumer [" << consumer_id << "] raw time per one item: " << avg_time_ns << "ns"
               << "total_time_ms=" << nanos / (1000 * 1000) << " consumed [" << totalMsgConsumed
               << " items \n";
          CHECK(totalMsgConsumed == N);
          CHECK(avg_time_ns < 150);
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception " << e.what();
        }
      }));
  }

  while (consumer_joined_num.load() < _MAX_CONSUMERS_)
    ;

  std::vector<std::thread> producers;
  for (auto queue_it = begin(queues); queue_it != end(queues); ++queue_it)
  {
    producers.emplace_back(std::thread(
      [&q = *queue_it, &consumer_joined_num, N]()
      {
        try
        {
          ProducerBlocking<Queue> p(q);
          q.start();
          auto begin = std::chrono::system_clock::now();
          size_t n = 1;
          while (n <= _MSG_PER_CONSUMER_)
          {
            if (p.emplace(OrderNonTrivial{n, 1U, 100.1, 'B'}) == ProduceReturnCode::Published)
              ++n;
          }

          q.stop();
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception " << e.what();
        }
      }));
  }

  for (auto& p : producers)
    p.join();

  for (auto& c : consumers)
    c.join();
}
TEST_CASE("SingleThreaded SPMC throughput test")
{
  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = 10000000;
  using Queue = SPMCMulticastQueueReliableBounded<OrderNonTrivial, _MAX_CONSUMERS_>;
  Queue q(_PUBLISHER_QUEUE_SIZE);

  size_t from = std::chrono::system_clock::now().time_since_epoch().count();
  std::vector<std::thread> consumers;
  std::vector<size_t> totalVols(_MAX_CONSUMERS_, 0);
  std::atomic_int consumer_joined_num{0};

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";
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
               << "total_time_ms=" << nanos / (1000 * 1000) << " consumed [" << totalVols[i] << " items \n";
          CHECK(totalVols[i] == N);
          CHECK(avg_time_ns < 150);
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception (10)" << e.what();
        }
      }));
  }

  while (consumer_joined_num.load() < _MAX_CONSUMERS_)
    ;

  std::thread producer(
    [&q, &consumer_joined_num, N]()
    {
      try
      {

        ProducerBlocking<Queue> p(q);
        q.start();
        auto begin = std::chrono::system_clock::now();
        size_t n = 1;
        while (n <= N)
        {
          if (p.emplace(OrderNonTrivial{n, 1U, 100.1, 'B'}) == ProduceReturnCode::Published)
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

#if defined(__x86_64__)
TEST_CASE("Conflated MPMC - consumers joining at random times")
{
  using Queue = SPMCMulticastQueueUnreliable<Order>;
  constexpr size_t _MAX_PUBLISHERS_ = 4;
  constexpr size_t _MAX_CONSUMERS_ = 2;
  constexpr size_t _MSG_PER_CONSUMER_ = 100000000;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = _MSG_PER_CONSUMER_ * _MAX_PUBLISHERS_;

  std::list<Queue> queues;
  for (size_t i = 0; i < _MAX_PUBLISHERS_; ++i)
  {
    queues.emplace_back(_PUBLISHER_QUEUE_SIZE);
    queues.back().start();
  }

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  size_t i = 0;
  std::vector<std::thread> producers;
  for (auto queueIt = queues.begin(); queueIt != std::end(queues); ++queueIt, ++i)
  {
    producers.emplace_back(std::thread(
      [producer_id = i, q = std::ref(*queueIt), N]() mutable
      {
        try
        {
          ProducerNonBlocking<Queue> p(q.get());
          q.get().start();
          for (size_t j = 1; j <= _MSG_PER_CONSUMER_; ++j)
          {
            if (ProduceReturnCode::Published == p.emplace(Order{j, 1U, 100.1, 'A'}))
            {
            }
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
      [&queues, i, N, &totalMsgConsumed]()
      {
        try
        {
          // each consumer thread attaches itself to each producer!
          auto queueIt = queues.begin();
          ConsumerNonBlocking<Queue> per_pub_consumer[_MAX_PUBLISHERS_]{*queueIt, *++queueIt,
                                                                        *++queueIt, *++queueIt};

          // make sure consumers join at diffrent times
          usleep(100000);

          auto begin = std::chrono::system_clock::now();
          bool finished = false;
          while (!finished)
          {
            for (size_t consumer_id = 0; consumer_id < _MAX_PUBLISHERS_; ++consumer_id)
            {
              bool is_consumer_done = false;
              auto r = per_pub_consumer[consumer_id].consume(
                [&](const Order& r) mutable
                {
                  totalMsgConsumed[i] += r.vol;
                  if (r.id >= _MSG_PER_CONSUMER_) // consumed all messages!
                    is_consumer_done = true;
                });
              if (r == ConsumeReturnCode::Stopped)
                is_consumer_done = true;

              if (is_consumer_done)
              {
                finished = true;
                break;
              }
            }
          }

          auto end = std::chrono::system_clock::now();
          auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
          auto avg_time_ns = (nanos.count() / totalMsgConsumed[i]);
          TLOG << "Consumer [" << i << "] Avg message time: " << avg_time_ns << "ns"
               << "total_time_ms=" << nanos / (1000 * 1000) << " consumed [" << totalMsgConsumed[i]
               << " items \n";
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

TEST_CASE("SingleThreaded Anycast MPMC throughput test")
{
  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _MAX_PUBLISHERS_ = 4;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t _MSG_PER_CONSUMER_ = 10000000;
  constexpr size_t N = _MAX_PUBLISHERS_ * _MSG_PER_CONSUMER_;

  using Queue = SPMCMulticastQueueReliableBounded<OrderNonTrivial, _MAX_CONSUMERS_>;
  std::list<Queue> queues;
  AnycastConsumerGroup<Queue> consumer_group;
  for (size_t i = 0; i < _MAX_PUBLISHERS_; ++i)
  {
    queues.emplace_back(_PUBLISHER_QUEUE_SIZE);
    consumer_group.attach(std::to_address(&queues.back()));
  }

  std::vector<std::thread> consumers;
  std::atomic_int consumer_joined_num{0};
  size_t totalMsgConsumed[_MAX_CONSUMERS_]{};

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";
  for (size_t consumer_id = 0; consumer_id < _MAX_CONSUMERS_; ++consumer_id)
  {
    consumers.push_back(std::thread(
      [&, id = consumer_id]()
      {
        try
        {
          AnycastConsumerBlocking<Queue> c(consumer_group);
          ++consumer_joined_num;
          auto begin = std::chrono::system_clock::now();
          while (1)
          {
            auto r = c.consume([&](const OrderNonTrivial& r) mutable { ++totalMsgConsumed[id]; });
            if (r == ConsumeReturnCode::Stopped)
              break;
          }

          REQUIRE(totalMsgConsumed[id] > 0);
          std::scoped_lock lock(guard);
          auto end = std::chrono::system_clock::now();
          auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
          auto avg_time_ns = (totalMsgConsumed[id] ? (nanos.count() / totalMsgConsumed[id]) : 0);
          TLOG << "Consumer [" << id << "] raw time per one item: " << avg_time_ns << "ns"
               << "total_time_ms=" << nanos.count() / (1000 * 1000) << " consumed ["
               << totalMsgConsumed[id] << "] items \n";
          CHECK(avg_time_ns < 300);
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception " << e.what();
        }
      }));
  }

  std::vector<std::thread> producers;
  for (auto queue_it = begin(queues); queue_it != end(queues); ++queue_it)
  {
    producers.emplace_back(std::thread(
      [&q = *queue_it, &consumer_joined_num, N]()
      {
        try
        {
          ProducerBlocking<Queue> p(q);
          q.start();
          size_t n = 1;
          while (n <= _MSG_PER_CONSUMER_)
          {
            if (p.emplace(OrderNonTrivial{n, 1U, 100.1, 'B'}) == ProduceReturnCode::Published)
              ++n;
          }

          q.stop();
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception " << e.what();
        }
      }));
  }

  for (auto& c : consumers)
    c.join();

  for (auto& p : producers)
    p.join();

  size_t actual_msg_consumed_num = std::accumulate(totalMsgConsumed, totalMsgConsumed + _MAX_CONSUMERS_, 0);
  TLOG << "\n total_msg_consumed_num=" << actual_msg_consumed_num << "\n";
  CHECK(actual_msg_consumed_num == N);
}

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }
