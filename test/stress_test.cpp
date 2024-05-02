#include "common_test_utils.h"
#include "detail/common.h"
#include <algorithm>
#include <assert.h>
#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#include <mpmc.h>
#include <numeric>
#include <thread>
#include <vector>

#if defined(__x86_64__)
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
  using Queue = SPMCBoundedConflatedQueue<Vector, ProducerKind::Unordered, _MAX_CONSUMERS_, BATCH_NUM>;
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
        while (ConsumerReturnCode::Stopped !=
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
  using Queue = SPMCBoundedQueue<Vector, ProducerKind::Unordered, _MAX_CONSUMERS_, BATCH_NUM>;
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
        while (ConsumerReturnCode::Stopped !=
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

TEST_CASE("SPMC sequential queue stress test to detect race conditions")
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
  constexpr size_t _PUBLISHER_QUEUE_SIZE = 16;
  constexpr size_t N = 3000000;
  constexpr size_t BATCH_NUM = 2;
  using Queue = SPMCBoundedQueue<Vector, ProducerKind::Sequential, _MAX_CONSUMERS_, BATCH_NUM>;
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
        while (ConsumerReturnCode::Stopped !=
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
