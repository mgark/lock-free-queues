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

TEST_CASE("SingleThreaded Anycast MPMC attach detach test")
{
  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _MAX_PUBLISHERS_ = 4;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t _ATTACH_DETACH_ITERATIONS_ = 3000;
  using Queue = SPMCMulticastQueueReliableBounded<OrderNonTrivial, _MAX_CONSUMERS_>;

  std::string s;
  std::mutex guard;
  std::deque<Queue> queues;
  for (size_t i = 0; i < _MAX_PUBLISHERS_; ++i)
    queues.emplace_back(_PUBLISHER_QUEUE_SIZE);

  AnycastConsumerGroup<Queue> consumer_group({std::to_address(&queues.front())});
  std::vector<std::thread> consumers;

  for (size_t consumer_id = 0; consumer_id < _MAX_CONSUMERS_; ++consumer_id)
  {
    consumers.push_back(std::thread(
      [&, id = consumer_id]()
      {
        try
        {
          std::srand(std::time(nullptr));
          AnycastConsumerBlocking<Queue> c(consumer_group);

          for (int i = 0; i < _ATTACH_DETACH_ITERATIONS_; ++i)
          {
            std::srand(std::time(nullptr));
            size_t idx = 1 + (std::rand() % (_MAX_PUBLISHERS_ - 1));
            bool attach = (i + consumer_id) & 1;
            if (id == 0 /*so that we always got at least one consumer*/ || attach)
            {
              consumer_group.detach(std::to_address(&queues[idx]));
              consumer_group.attach(std::to_address(&queues[idx]));
            }
            else
            {
              consumer_group.detach(std::to_address(&queues[idx]));
            }

            int j = 0;
            size_t msg_consumed = 0;
            while (j < 1000)
            {
              auto r = c.consume([&](const OrderNonTrivial& r) mutable { ++msg_consumed; });
              if (r == ConsumeReturnCode::Consumed)
              {
                ++j;
              }
              else
              {
                break;
              }
            }

            CHECK(msg_consumed > 0);
          }
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
      [&q = *queue_it]()
      {
        try
        {
          ProducerBlocking<Queue> p(q);
          q.start();

          size_t n = 1;
          while (1)
          {
            if (p.emplace(OrderNonTrivial{n, 1U, 100.1, 'B'}) == ProduceReturnCode::NotRunning)
              break;
          }
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception " << e.what();
        }
      }));
  }

  for (auto& c : consumers)
    c.join();

  TLOG << "\n all consumers are done\n";

  for (auto& q : queues)
    q.stop();

  for (auto& p : producers)
    p.join();
}

TEST_CASE("Multi-threaded Anycast MPMC attach detach test")
{
  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _MAX_PUBLISHERS_ = 4;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 64;
  constexpr size_t _ATTACH_DETACH_ITERATIONS_ = 10000;
  using Queue = SPMCMulticastQueueReliableBounded<OrderNonTrivial, _MAX_CONSUMERS_>;

  std::string s;
  std::mutex guard;
  std::deque<Queue> queues;
  queues.emplace_back(_PUBLISHER_QUEUE_SIZE);
  queues.emplace_back(_PUBLISHER_QUEUE_SIZE);
  // Queue queue2(_PUBLISHER_QUEUE_SIZE);

  AnycastConsumerGroup<Queue> consumer_group({std::to_address(&queues[0]), std::to_address(&queues[1])});
  std::vector<std::thread> consumers;

  for (size_t consumer_id = 0; consumer_id < _MAX_CONSUMERS_; ++consumer_id)
  {
    consumers.push_back(std::thread(
      [&, id = consumer_id]()
      {
        try
        {
          std::srand(std::time(nullptr));
          AnycastConsumerBlocking<Queue> c(consumer_group);
          for (int i = 0; i < _ATTACH_DETACH_ITERATIONS_; ++i)
          {
            std::srand(std::time(nullptr));
            bool attach = !(i % 10 == 0);
            size_t queue_idx = i % queues.size();

            if (attach)
            {
              // consumer_group.detach(std::to_address(&queue));
              consumer_group.attach(std::to_address(&queues[queue_idx]));
            }
            else
            {
              consumer_group.detach(std::to_address(&queues[queue_idx]));
            }

            int j = 0;
            size_t msg_consumed = 0;
            while (j < 5000)
            {
              auto r = c.consume([&](const OrderNonTrivial& r) mutable { ++msg_consumed; });
              if (r == ConsumeReturnCode::Consumed)
              {
                ++j;
              }
              else
              {
                break;
              }
            }

            // CHECK(msg_consumed > 0);
          }
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception " << e.what();
        }
      }));
  }

  std::vector<std::thread> producers;
  ProducerSynchronizedContext producer_group_1;
  ProducerSynchronizedContext producer_group_2;
  queues[0].start();
  queues[1].start();
  for (size_t i = 1; i <= _MAX_PUBLISHERS_; ++i)
  {
    producers.emplace_back(std::thread(
      [&queues, &producer_group_1, &producer_group_2]()
      {
        try
        {
          ProducerBlocking<Queue, ProducerKind::Synchronized> p1(queues[0], producer_group_1);

          size_t n = 1;
          while (1)
          {
            if (p1.emplace(OrderNonTrivial{n, 1U, 100.1, 'B'}) == ProduceReturnCode::NotRunning)
              break;
          }
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception " << e.what();
        }
      }));
  }

  for (size_t i = 1; i <= _MAX_PUBLISHERS_; ++i)
  {
    producers.emplace_back(std::thread(
      [&queues, &producer_group_2]()
      {
        try
        {
          ProducerBlocking<Queue, ProducerKind::Synchronized> p2(queues[1], producer_group_2);

          size_t n = 1;
          while (1)
          {
            if (p2.emplace(OrderNonTrivial{n, 1U, 100.1, 'B'}) == ProduceReturnCode::NotRunning)
              break;
          }
        }
        catch (const std::exception& e)
        {
          TLOG << "\n got exception " << e.what();
        }
      }));
  }

  for (auto& c : consumers)
    c.join();

  TLOG << "\n all consumers are done\n";
  queues[0].stop();
  queues[1].stop();

  for (auto& p : producers)
    p.join();
}

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }