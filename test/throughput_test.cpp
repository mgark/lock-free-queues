#include <catch2/catch_all.hpp>
#include <iostream>
#include <list>
#include <mpmc.h>
#include <mutex>
#include <random>
#include <thread>

#include "utils.h"

struct Order
{
  size_t id;
  size_t vol;
  double price;
  char side;
};

struct OrderNonTrivial
{
  size_t id;
  size_t vol;
  double price;
  char side;

  ~OrderNonTrivial() {}
};

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
        Consumer<Queue, true> c(q);
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

      Producer<Queue, true> p(q);
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
        Consumer<Queue, true> c(q);
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

      Producer<Queue, true> p(q);
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

TEST_CASE("Unordered MPMC throughput test")
{
  using Queue = SPMCBoundedQueue<OrderNonTrivial>;
  constexpr size_t _MAX_PUBLISHERS_ = 4;
  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _MSG_PER_CONSUMER_ = 10000000;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = _MSG_PER_CONSUMER_ * _MAX_PUBLISHERS_;

  std::list<Queue> publishersQueues;
  for (size_t i = 0; i < _MAX_PUBLISHERS_; ++i)
    publishersQueues.emplace_back(_PUBLISHER_QUEUE_SIZE);

  TLOG << "\n\n";
  size_t i = 0;
  std::vector<std::thread> consumers;
  std::vector<size_t> totalVols(_MAX_CONSUMERS_, 0);
  std::atomic_int consumer_joined_num{0};
  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&publishersQueues, &consumer_joined_num, i, N, &totalVols]()
      {
        // each consumer thread attaches itself to each producer!
        auto producerIt = publishersQueues.begin();
        Consumer<Queue, false> per_pub_consumer[_MAX_PUBLISHERS_]{*producerIt, *++producerIt,
                                                                  *++producerIt, *++producerIt};

        consumer_joined_num.fetch_add(_MAX_PUBLISHERS_);
        size_t consumed_msg_num = 0;
        auto process_msg = [consumer_id = i, &consumed_msg_num, &totalVols](auto& c, bool& set_true_when_done)
        {
          return c.consume(
            [consumer_id, &consumed_msg_num, &set_true_when_done, &totalVols](const OrderNonTrivial& r) mutable
            {
              totalVols[consumer_id] += r.vol;
              if (r.id >= _MSG_PER_CONSUMER_) // consumed all messages!
              {
                consumed_msg_num += _MSG_PER_CONSUMER_;
                set_true_when_done = true;
              }
            });
        };

        bool done[_MAX_PUBLISHERS_]{false};
        auto begin = std::chrono::system_clock::now();
        while (consumed_msg_num < N)
        {
          for (size_t j = 0; j < _MAX_PUBLISHERS_; ++j)
          {
            if (!done[j])
            {
              process_msg(per_pub_consumer[j], done[j]);
            }
          }
        }

        auto end = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
        auto avg_time_ns = (nanos.count() / totalVols[i]);

        TLOG << "Consumer [" << i << "] Avg message time: " << avg_time_ns << "ns"
             << " consumed [" << totalVols[i] << " items \n";

        CHECK(N == totalVols[i]);
        CHECK(avg_time_ns < 20);
      }));
  }

  while (consumer_joined_num < _MAX_CONSUMERS_ * _MAX_PUBLISHERS_)
    ;

  std::vector<std::thread> producers;
  for (auto publisherIt = publishersQueues.begin(); publisherIt != std::end(publishersQueues);
       ++publisherIt, ++i)
  {
    producers.emplace_back(std::thread(
      [producer_id = i, q = std::ref(*publisherIt), N]() mutable
      {
        Producer<Queue, true> p(q.get());
        for (size_t i = 1; i <= _MSG_PER_CONSUMER_; ++i)
          p.emplace(OrderNonTrivial{i, 1U, 100.1, 'A'});
      }));
  }

  for (auto& p : producers)
    p.join();
  for (auto& c : consumers)
    c.join();
}

TEST_CASE("Unordered MPMC - consumers joining at random times")
{
  using Queue = SPMCBoundedQueue<OrderNonTrivial>;
  constexpr size_t _MAX_PUBLISHERS_ = 4;
  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _MSG_PER_CONSUMER_ = 100000000;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = _MSG_PER_CONSUMER_ * _MAX_PUBLISHERS_;

  std::list<Queue> publishersQueues;
  for (size_t i = 0; i < _MAX_PUBLISHERS_; ++i)
    publishersQueues.emplace_back(_PUBLISHER_QUEUE_SIZE);

  TLOG << "\nUnordered MPMC - consumers joining at random times\n";

  size_t i = 0;
  std::vector<std::thread> producers;
  for (auto publisherIt = publishersQueues.begin(); publisherIt != std::end(publishersQueues);
       ++publisherIt, ++i)
  {
    producers.emplace_back(std::thread(
      [producer_id = i, q = std::ref(*publisherIt), N]() mutable
      {
        Producer<Queue, true> p(q.get());
        for (size_t i = 1; i <= _MSG_PER_CONSUMER_; ++i)
        {
          // std::cout << "return code = " << (int)p.emplace(OrderNonTrivial{i, 1U, 100.1, 'A'});
          p.emplace(OrderNonTrivial{i, 1U, 100.1, 'A'});
          // std::cout << "zn" << std::endl;
        }

        q.get().stop();
      }));
  }

  // make sure producer joins first!
  usleep(100000);
  std::vector<std::thread> consumers;
  std::vector<size_t> totalVols(_MAX_CONSUMERS_, 0);

  for (i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&publishersQueues, i, N, &totalVols]()
      {
        // each consumer thread attaches itself to each producer!
        auto queueIt = publishersQueues.begin();
        Consumer<Queue, false> per_pub_consumer[_MAX_PUBLISHERS_]{*queueIt, *++queueIt, *++queueIt, *++queueIt};

        // make sure consumers join at diffrent times
        usleep(100000);
        std::cout << std::endl;

        size_t consumed_msg_num = 0;
        auto process_msg = [consumer_id = i, &consumed_msg_num, &totalVols](auto& c, bool& set_true_when_done)
        {
          return c.consume(
            [consumer_id, &consumed_msg_num, &set_true_when_done, &totalVols](const OrderNonTrivial& r) mutable
            {
              // TLOG << "consumer msg\n";
              totalVols[consumer_id] += r.vol;
              if (r.id >= _MSG_PER_CONSUMER_) // consumed all messages!
              {
                consumed_msg_num += _MSG_PER_CONSUMER_;
                set_true_when_done = true;
              }
            });
        };

        bool done[_MAX_PUBLISHERS_]{false};
        auto begin = std::chrono::system_clock::now();
        while (consumed_msg_num < N)
        {
          for (size_t j = 0; j < _MAX_PUBLISHERS_; ++j)
          {
            if (!done[j])
            {
              if (ConsumerReturnCode::Consumed != process_msg(per_pub_consumer[j], done[j]))
              {
                break;
              }
            }
          }
        }

        auto end = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
        auto avg_time_ns = (nanos.count() / totalVols[i]);

        TLOG << "Consumer [" << i << "] Avg message time: " << avg_time_ns << "ns"
             << " consumed [" << totalVols[i] << " items \n";

        // CHECK(avg_time_ns < 20);
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

  TLOG << "\n MPMC SEQUENTIAL \n";

  std::vector<std::thread> consumers;
  std::vector<size_t> totalVols(_MAX_CONSUMERS_, 0);
  std::atomic_int consumer_joined_num{0};

  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&q, i, N, &consumer_joined_num, &totalVols]()
      {
        size_t consumed_msg_num = 0;
        Consumer<Queue, true> consumer(q);
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

        auto end = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
        auto avg_time_ns = nanos.count() / totalVols[i];
        TLOG << "Consumer [" << i << "] raw latency: " << avg_time_ns << "ns"
             << " consumed [" << totalVols[i] << " items \n";
        CHECK(N == totalVols[i]);
        CHECK(avg_time_ns < 300);
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
        Producer<Queue, true> p(q.get());
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

        auto end = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
        std::cout << "[" << producer_id << "] Produced " << N << " times " << (nanos.count() / N) << "ns\n";
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

TEST_CASE("Unordered conflated MPMC - consumers joining at random times")
{
  using Queue = SPMCBoundedConflatedQueue<Order>;
  constexpr size_t _MAX_PUBLISHERS_ = 4;
  constexpr size_t _MAX_CONSUMERS_ = 2;
  constexpr size_t _MSG_PER_CONSUMER_ = 100000000;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1024;
  constexpr size_t N = _MSG_PER_CONSUMER_ * _MAX_PUBLISHERS_;

  std::list<Queue> publishersQueues;
  for (size_t i = 0; i < _MAX_PUBLISHERS_; ++i)
    publishersQueues.emplace_back(_PUBLISHER_QUEUE_SIZE);

  TLOG << "\nUnordered conflated MPMC - consumers joining at random times\n";

  size_t i = 0;
  std::vector<std::thread> producers;
  for (auto publisherIt = publishersQueues.begin(); publisherIt != std::end(publishersQueues);
       ++publisherIt, ++i)
  {
    producers.emplace_back(std::thread(
      [producer_id = i, q = std::ref(*publisherIt), N]() mutable
      {
        Producer<Queue> p(q.get());
        for (size_t i = 1; i <= _MSG_PER_CONSUMER_; ++i)
        {
          // std::cout << "return code = " << (int)p.emplace(OrderNonTrivial{i, 1U, 100.1, 'A'});
          p.emplace(Order{i, 1U, 100.1, 'A'});
          // std::cout << "zn" << std::endl;
        }

        q.get().stop();
      }));
  }

  // make sure producer joins first!
  usleep(100000);
  std::vector<std::thread> consumers;
  std::vector<size_t> totalVols(_MAX_CONSUMERS_, 0);

  for (i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&publishersQueues, i, N, &totalVols]()
      {
        // each consumer thread attaches itself to each producer!
        auto queueIt = publishersQueues.begin();
        Consumer<Queue, false> per_pub_consumer[_MAX_PUBLISHERS_]{*queueIt, *++queueIt, *++queueIt, *++queueIt};

        // make sure consumers join at diffrent times
        usleep(100000);
        std::cout << std::endl;

        size_t consumed_msg_num = 0;
        auto process_msg = [consumer_id = i, &consumed_msg_num, &totalVols](auto& c, bool& set_true_when_done)
        {
          return c.consume(
            [consumer_id, &consumed_msg_num, &set_true_when_done, &totalVols](const Order& r) mutable
            {
              // TLOG << "consumer msg\n";
              totalVols[consumer_id] += r.vol;
              if (r.id >= _MSG_PER_CONSUMER_) // consumed all messages!
              {
                consumed_msg_num += _MSG_PER_CONSUMER_;
                set_true_when_done = true;
              }
            });
        };

        bool done[_MAX_PUBLISHERS_]{false};
        auto begin = std::chrono::system_clock::now();
        while (consumed_msg_num < N)
        {
          for (size_t j = 0; j < _MAX_PUBLISHERS_; ++j)
          {
            if (!done[j])
            {
              if (ConsumerReturnCode::Consumed != process_msg(per_pub_consumer[j], done[j]))
              {
                break;
              }
            }
          }
        }

        auto end = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin);
        auto avg_time_ns = (nanos.count() / totalVols[i]);

        TLOG << "Consumer [" << i << "] Avg message time: " << avg_time_ns << "ns"
             << " consumed [" << totalVols[i] << " items \n";

        // CHECK(avg_time_ns < 20);
      }));
  }

  for (auto& p : producers)
    p.join();
  for (auto& c : consumers)
    c.join();
}

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }
