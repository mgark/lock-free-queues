#include "common_test_utils.h"
#include "detail/common.h"
#include "detail/producer.h"
#include "detail/single_bit_reuse.h"
#include <algorithm>
#include <assert.h>
#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#include <mpmc.h>
#include <numeric>
#include <thread>
#include <vector>

TEST_CASE(
  "Bounded blocking reliable multicast SPSC attach detach & stress test - SingleThreaded Queue "
  "config")
{
  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 1;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 4;
  constexpr size_t N = 10'000'000;
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

TEST_CASE(
  "Bounded blocking reliable multicast SPSC attach detach & stress test - MultiThreaded Queue "
  "config")
{
  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 1;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 4;
  constexpr size_t N = 10'000'000;
  constexpr size_t ATTACH_DETACH_ITERATIONS = 20000;
  constexpr size_t CONSUMED_PER_ITERATION = N / ATTACH_DETACH_ITERATIONS / 100;
  using Queue = SPMCMulticastQueueReliableBounded<size_t, 3, 3>;
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
  constexpr size_t _MAX_CONSUMERS_ = 8;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 4;
  constexpr size_t N_PER_CONSUMER = 20000;
  constexpr size_t ATTACH_DETACH_ITERATIONS = 3000;
  constexpr size_t CONSUMED_PER_ITERATION = N_PER_CONSUMER / ATTACH_DETACH_ITERATIONS;
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

                // if (consumer_idx == 0)
                //   TLOG << "\n consumed " << idx;

                if (0 == consumed_num++)
                {
                  // on the very first item, let's calculate our target checksum
                  size_t extra_checksum = 0;
                  for (size_t k = idx; k < idx + CONSUMED_PER_ITERATION; ++k)
                    extra_checksum += k;

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
        TLOG << "\n finished consumer = " << i;
      }));
  }
  std::thread producer(
    [&q, &consumer_finished_num, N_PER_CONSUMER]()
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
    TLOG << "\n published items = " << q.get_producer_idx() << " items to consume = " << N_PER_CONSUMER
         << " consumers_checksum=" << consumers_checksum << " producers_checksum=" << producers_checksum;
#endif

    if (consumers_checksum != producers_checksum)
      std::abort();
  } while (consumers_checksum != producers_checksum);
}

TEST_CASE("Bounded blocking reliable anycast MPMC attach detach & stress test")
{
  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  constexpr size_t _MAX_CONSUMERS_ = 8;
  constexpr size_t _MAX_PUBLISHERS_ = 8;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 4;
  constexpr size_t _ATTACH_DETACH_ITERATIONS_ = 1000;
  constexpr size_t _N_PER_ITERATION_ = 3;
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
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 4;
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
        for (int j = 0; j < _ATTACH_DETACH_ITERATIONS_; ++j)
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

TEST_CASE(
  "Bounded blocking reliable synthetic anycast from many queues MPMC attach detach & stress test - "
  "SingleThreaded queues")
{
  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  constexpr size_t _MAX_CONSUMERS_ = 8;
  constexpr size_t _MAX_PUBLISHERS_ = 8;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 4;
  constexpr size_t _ATTACH_DETACH_ITERATIONS_ = 1000;
  constexpr size_t _N_PER_CONSUMER_ = 20000;
  constexpr size_t _N_TO_CONSUME_ = _N_PER_CONSUMER_ * _MAX_CONSUMERS_;
  constexpr size_t _N_PER_ITERATION_ = _N_PER_CONSUMER_ / _ATTACH_DETACH_ITERATIONS_;
  constexpr bool _MULTICAST_ = true;

  using Queue = SPMCMulticastQueueReliableBounded<size_t, 1, 1, 4, _MULTICAST_>;

  std::string s;
  std::mutex guard;
  std::deque<Queue> queues;
  for (size_t i = 0; i < _MAX_PUBLISHERS_; ++i)
    queues.emplace_back(_PUBLISHER_QUEUE_SIZE);

  AnycastConsumerGroup<Queue> consumer_group({std::to_address(&queues[0]), std::to_address(&queues[1]),
                                              std::to_address(&queues[2]), std::to_address(&queues[3])});
  std::vector<std::thread> consumers;
  std::srand(std::time(nullptr));

  std::array<std::atomic<size_t>, _MAX_CONSUMERS_> per_consumer_sum{};
  std::array<std::atomic<size_t>, _MAX_CONSUMERS_> per_consumer_num{};
  std::vector<std::thread> consumer_threads;
  std::atomic_size_t consumers_attached{};
  for (size_t consumer_id = 0; consumer_id < _MAX_CONSUMERS_; ++consumer_id)
  {
    consumer_threads.push_back(std::thread(
      [&, id = consumer_id]()
      {
        try
        {
          AnycastConsumerBlocking<Queue> c(consumer_group);
          ++consumers_attached;
          while (consumers_attached < _MAX_CONSUMERS_)
            ;

          size_t i = 0;
          size_t msg_consumed = 0;
          bool done = false;
          for (int i = 0; i < _ATTACH_DETACH_ITERATIONS_; ++i)
          {
            size_t idx = 1 + (std::rand() % (_MAX_PUBLISHERS_ - 1));
            bool attach = (i + id) & 1;
            if (id != 0)
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

            size_t j = 0;
            while (j < _N_PER_ITERATION_)
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
                  ++j;
                });
            }
          }

          per_consumer_num[id] = msg_consumed;
          TLOG << " \n consumer is done id = " << id;
          TLOG.flush();
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
      [&q = *queue_it, &consumers_attached]()
      {
        try
        {
          while (consumers_attached < _MAX_CONSUMERS_)
            ;

          ProducerBlocking<Queue> p(q);
          q.start();

          size_t n = 0;
          while (1)
          {
            auto r = p.emplace_idx();
            if (r == ProduceReturnCode::Published)
              ++n;
            else if (r == ProduceReturnCode::NotRunning)
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

  size_t total_consumed_num = 0;
  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
    total_consumed_num += per_consumer_num[i];

  TLOG << "\n total consumed num = " << total_consumed_num << " max_total_consumed_num=" << _N_TO_CONSUME_;
}

TEST_CASE(
  "Bounded blocking reliable synthetic anycast from many queues MPMC attach detach & stress test - "
  "MultiThreaded queues")
{
  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";

  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _MAX_PUBLISHERS_ = 4;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 64;
  constexpr size_t _ATTACH_DETACH_ITERATIONS_ = 20000;
  constexpr size_t _N_PER_CONSUMER_ = 1000000;
  constexpr size_t _N_TO_CONSUME_ = _N_PER_CONSUMER_ * _MAX_CONSUMERS_;
  constexpr size_t _N_PER_ITERATION_ = _N_PER_CONSUMER_ / _ATTACH_DETACH_ITERATIONS_;
  constexpr bool _MULTICAST_ = true;

  using Queue = SPMCMulticastQueueReliableBounded<size_t, 4, 4, 4, _MULTICAST_>;

  std::string s;
  std::mutex guard;
  std::deque<Queue> queues;
  for (size_t i = 0; i < _MAX_PUBLISHERS_; ++i)
    queues.emplace_back(_PUBLISHER_QUEUE_SIZE);

  AnycastConsumerGroup<Queue> consumer_group({std::to_address(&queues[0]), std::to_address(&queues[1]),
                                              std::to_address(&queues[2]), std::to_address(&queues[3])});
  std::vector<std::thread> consumers;
  std::srand(std::time(nullptr));

  std::array<std::atomic<size_t>, _MAX_CONSUMERS_> per_consumer_sum{};
  std::array<std::atomic<size_t>, _MAX_CONSUMERS_> per_consumer_num{};
  std::vector<std::thread> consumer_threads;
  std::atomic_size_t consumers_attached{};
  for (size_t consumer_id = 0; consumer_id < _MAX_CONSUMERS_; ++consumer_id)
  {
    consumer_threads.push_back(std::thread(
      [&, id = consumer_id]()
      {
        try
        {
          AnycastConsumerBlocking<Queue> c(consumer_group);
          ++consumers_attached;
          while (consumers_attached < _MAX_CONSUMERS_)
            ;

          size_t i = 0;
          size_t msg_consumed = 0;
          bool done = false;
          for (int i = 0; i < _ATTACH_DETACH_ITERATIONS_; ++i)
          {
            size_t idx = 1 + (std::rand() % (_MAX_PUBLISHERS_ - 1));
            bool attach = (i + id) & 1;
            if (id != 0)
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

            size_t j = 0;
            while (j < _N_PER_ITERATION_)
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
                  ++j;
                });
            }
          }

          per_consumer_num[id] = msg_consumed;
          TLOG << " \n consumer is done id = " << id;
          TLOG.flush();
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
      [&q = *queue_it, &consumers_attached]()
      {
        try
        {
          while (consumers_attached < _MAX_CONSUMERS_)
            ;

          ProducerBlocking<Queue> p(q);
          q.start();

          size_t n = 0;
          while (1)
          {
            auto r = p.emplace_idx();
            if (r == ProduceReturnCode::Published)
              ++n;
            else if (r == ProduceReturnCode::NotRunning)
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

  size_t total_consumed_num = 0;
  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
    total_consumed_num += per_consumer_num[i];

  TLOG << "\n total consumed num = " << total_consumed_num << " max_total_consumed_num=" << _N_TO_CONSUME_;
}

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

#if defined(__x86_64__)
  #ifndef _DISABLE_UNRELIABLE_MULTICAST_TEST_
TEST_CASE("SPMC conflated queue stress test to detect race conditions")
{
  struct Vector
  {
    bool odd;
    int64_t v1[10];
  };

  int i = 1;
  Vector odd_vector;
  odd_vector.odd = true;
  for (int64_t& v : odd_vector.v1)
    v = i++;

  Vector even_vector;
  even_vector.odd = false;
  for (int64_t& v : even_vector.v1)
    v = --i;

  int sum = ((10 + 1) * 10) / 2;
  REQUIRE(std::accumulate(std::begin(odd_vector.v1), std::end(odd_vector.v1), 0) == sum);
  REQUIRE(std::accumulate(std::begin(even_vector.v1), std::end(even_vector.v1), 0) == sum);

  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 6;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 1;
  constexpr size_t N = 300000000;
  constexpr size_t BATCH_NUM = 1;
  using Queue = SPMCMulticastQueueUnreliable<Vector, _MAX_CONSUMERS_, BATCH_NUM>;
  Queue q(_PUBLISHER_QUEUE_SIZE);
  ProducerBlocking<Queue> p(q);

  size_t from = std::chrono::system_clock::now().time_since_epoch().count();
  std::vector<std::thread> consumers;
  std::vector<size_t> totalVols(_MAX_CONSUMERS_, 0);
  std::atomic_int consumer_joined_num{0};

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";
  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&q, i, &guard, N, sum, &consumer_joined_num, &totalVols]()
      {
        ConsumerBlocking<Queue> c(q);
        size_t n = 0;
        ++consumer_joined_num;
        auto begin = std::chrono::system_clock::now();
        while (ConsumeReturnCode::Stopped !=
               c.consume(
                 [consumer_id = i, sum, &n, &q, &totalVols](const Vector& r) mutable
                 {
                   // interleaving update won't make the sum equal to the target
                   CHECK(std::accumulate(std::begin(r.v1), std::end(r.v1), 0) == sum);
                   ++n;
                 }))
          ;
        TLOG << "Consumer [" << i << "]  processed  " << n << " updates\n";
      }));
  }

  std::thread producer(
    [&q, &p, &odd_vector, &even_vector, &consumer_joined_num, N]()
    {
      while (consumer_joined_num.load() < _MAX_CONSUMERS_)
        ;

      auto begin = std::chrono::system_clock::now();
      size_t n = 1;
      while (n <= N)
      {
        if ((n & 1) == 0)
        {
          p.emplace(even_vector);
        }
        else
        {
          p.emplace(odd_vector);
        }
        ++n;
      }

      q.stop();
    });

  producer.join();
  for (auto& c : consumers)
  {
    c.join();
  }
}
  #endif
#endif

TEST_CASE("SPMC queue stress test to detect race conditions")
{
  struct Vector
  {
    bool odd;
    int64_t v1[10];
  };

  int i = 1;
  Vector odd_vector;
  odd_vector.odd = true;
  for (int64_t& v : odd_vector.v1)
    v = i++;

  Vector even_vector;
  even_vector.odd = false;
  for (int64_t& v : even_vector.v1)
    v = --i;

  int sum = ((10 + 1) * 10) / 2;
  REQUIRE(std::accumulate(std::begin(odd_vector.v1), std::end(odd_vector.v1), 0) == sum);
  REQUIRE(std::accumulate(std::begin(even_vector.v1), std::end(even_vector.v1), 0) == sum);

  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 32;
  constexpr size_t N = 30000000;
  constexpr size_t BATCH_NUM = 2;
  using Queue = SPMCMulticastQueueReliableBounded<Vector, _MAX_CONSUMERS_, BATCH_NUM>;
  Queue q(_PUBLISHER_QUEUE_SIZE);

  size_t from;
  std::vector<std::thread> consumers;
  std::atomic_int consumer_joined_num{0};

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";
  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&q, i, &guard, N, sum, &consumer_joined_num]()
      {
        ConsumerBlocking<Queue> c(q);
        ++consumer_joined_num;
        size_t n = 0;
        auto begin = std::chrono::system_clock::now();
        while (ConsumeReturnCode::Stopped !=
               c.consume(
                 [consumer_id = i, sum, &n](const Vector& r) mutable
                 {
                   // interleaving update won't make the sum equal to the target
                   CHECK(std::accumulate(std::begin(r.v1), std::end(r.v1), 0) == sum);
                   ++n;
                 }))
          ;
        TLOG << "Consumer [" << i << "]  processed  " << n << " updates\n";
      }));
  }

  while (consumer_joined_num.load() < _MAX_CONSUMERS_)
    ;
  q.start();

  std::thread producer(
    [&q, &from, &odd_vector, &even_vector, &consumer_joined_num, N]()
    {
      ProducerBlocking<Queue> p(q);
      from = std::chrono::system_clock::now().time_since_epoch().count();
      size_t n = 1;
      while (n <= N)
      {
        if ((n & 1) == 0)
        {
          p.emplace(even_vector);
        }
        else
        {
          p.emplace(odd_vector);
        }
        ++n;
      }

      q.stop();
    });

  producer.join();
  for (auto& c : consumers)
  {
    c.join();
  }
}

TEST_CASE("SPMC Synchronized queue stress test to detect race conditions")
{
  // TODO: fix to support multie producers!
  struct Vector
  {
    bool odd;
    int64_t v1[10];
  };

  int i = 1;
  Vector odd_vector;
  odd_vector.odd = true;
  for (int64_t& v : odd_vector.v1)
    v = i++;

  Vector even_vector;
  even_vector.odd = false;
  for (int64_t& v : even_vector.v1)
    v = --i;

  int sum = ((10 + 1) * 10) / 2;
  REQUIRE(std::accumulate(std::begin(odd_vector.v1), std::end(odd_vector.v1), 0) == sum);
  REQUIRE(std::accumulate(std::begin(even_vector.v1), std::end(even_vector.v1), 0) == sum);

  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 16;
  constexpr size_t N = 3000000;
  constexpr size_t BATCH_NUM = 2;
  using Queue = SPMCMulticastQueueReliableBounded<Vector, _MAX_CONSUMERS_, BATCH_NUM>;
  Queue q(_PUBLISHER_QUEUE_SIZE);

  size_t from;
  std::vector<std::thread> consumers;
  std::atomic_int consumer_joined_num{0};

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";
  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&q, i, &guard, N, sum, &consumer_joined_num]()
      {
        ConsumerBlocking<Queue> c(q);
        size_t n = 0;
        ++consumer_joined_num;
        auto begin = std::chrono::system_clock::now();
        while (ConsumeReturnCode::Stopped !=
               c.consume(
                 [consumer_id = i, sum, &n](const Vector& r) mutable
                 {
                   // interleaving update won't make the sum equal to the target
                   CHECK(std::accumulate(std::begin(r.v1), std::end(r.v1), 0) == sum);
                   ++n;
                 }))
          ;
        TLOG << "Consumer [" << i << "]  processed  " << n << " updates\n";
      }));
  }

  std::thread producer(
    [&q, &from, &odd_vector, &even_vector, &consumer_joined_num, N]()
    {
      while (consumer_joined_num.load() < _MAX_CONSUMERS_)
        ;

      ProducerBlocking<Queue> p(q);
      q.start();
      from = std::chrono::system_clock::now().time_since_epoch().count();
      size_t n = 1;
      while (n <= N)
      {
        if ((n & 1) == 0)
        {
          p.emplace(even_vector);
        }
        else
        {
          p.emplace(odd_vector);
        }
        ++n;
      }

      q.stop();
    });

  producer.join();
  for (auto& c : consumers)
  {
    c.join();
  }
}

TEST_CASE("Adaptive SPMC  queue stress test to detect race conditions")
{
  // TODO: fix to support multie producers!
  struct Vector
  {
    bool odd;
    int64_t v1[10];
  };

  int i = 1;
  Vector odd_vector;
  odd_vector.odd = true;
  for (int64_t& v : odd_vector.v1)
    v = i++;

  Vector even_vector;
  even_vector.odd = false;
  for (int64_t& v : even_vector.v1)
    v = --i;

  int sum = ((10 + 1) * 10) / 2;
  REQUIRE(std::accumulate(std::begin(odd_vector.v1), std::end(odd_vector.v1), 0) == sum);
  REQUIRE(std::accumulate(std::begin(even_vector.v1), std::end(even_vector.v1), 0) == sum);

  std::string s;
  std::mutex guard;
  constexpr size_t _MAX_CONSUMERS_ = 3;
  constexpr size_t _PUBLISHER_INITIAL_QUEUE_SIZE = 16;
  constexpr size_t _PUBLISHER_MAX_QUEUE_SIZE = 1024 * 1024;
  constexpr size_t N = 3000000;
  constexpr size_t BATCH_NUM = 2;
  using Queue = SPMCMulticastQueueReliableAdaptiveBounded<Vector, _MAX_CONSUMERS_, BATCH_NUM>;
  Queue q(_PUBLISHER_INITIAL_QUEUE_SIZE, _PUBLISHER_INITIAL_QUEUE_SIZE);

  size_t from;
  std::vector<std::thread> consumers;
  std::atomic_int consumer_joined_num{0};

  TLOG << "\n  " << Catch::getResultCapture().getCurrentTestName() << "\n";
  for (size_t i = 0; i < _MAX_CONSUMERS_; ++i)
  {
    consumers.push_back(std::thread(
      [&q, i, &guard, N, sum, &consumer_joined_num]()
      {
        ConsumerBlocking<Queue> c(q);
        size_t n = 0;
        ++consumer_joined_num;
        auto begin = std::chrono::system_clock::now();
        while (ConsumeReturnCode::Stopped !=
               c.consume(
                 [consumer_id = i, sum, &n](const Vector& r) mutable
                 {
                   // interleaving update won't make the sum equal to the target
                   CHECK(std::accumulate(std::begin(r.v1), std::end(r.v1), 0) == sum);
                   ++n;
                 }))
          ;
        TLOG << "Consumer [" << i << "]  processed  " << n << " updates\n";
      }));
  }

  std::thread producer(
    [&q, &from, &odd_vector, &even_vector, &consumer_joined_num, N]()
    {
      while (consumer_joined_num.load() < _MAX_CONSUMERS_)
        ;

      ProducerBlocking<Queue> p(q);
      q.start();
      from = std::chrono::system_clock::now().time_since_epoch().count();
      size_t n = 1;
      while (n <= N)
      {
        if ((n & 1) == 0)
        {
          p.emplace(even_vector);
        }
        else
        {
          p.emplace(odd_vector);
        }
        ++n;
      }

      q.stop();
    });

  producer.join();
  for (auto& c : consumers)
  {
    c.join();
  }
}

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }
