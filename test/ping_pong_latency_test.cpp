#include "common_test_utils.h"
#include "detail/common.h"
#include <assert.h>
#include <atomic>
#include <catch2/catch_all.hpp>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mpmc.h>

#include <new>
#include <thread>
#include <type_traits>
#include <x86intrin.h>

template <class T>
class SPSC2
{
  struct Node
  {
    alignas(T) std::byte paylod[sizeof(T)];
    std::atomic_bool busy{false};
  };

  static_assert(std::is_trivially_copyable_v<T>);
  static_assert(std::is_trivially_destructible_v<T>);

  Node* data_;
  alignas(128) size_t read_idx_{0};
  alignas(128) size_t write_idx_{0};
  std::allocator<Node> alloc_;
  size_t N_;

public:
  SPSC2(size_t N) : N_(N)
  {
    data_ = alloc_.allocate(N_);
    for (int i = 0; i < N_; ++i)
    {
      std::uninitialized_default_construct(data_, data_ + N_);
    }
  }

  ~SPSC2() { alloc_.deallocate(data_, N_); }

  template <class F>
  void consume(F&& f)
  {
    Node* node = &data_[read_idx_];
    while (!node->busy.load(std::memory_order_acquire))
      ;
    std::forward<F>(f)(*std::launder(reinterpret_cast<T*>(node->paylod)));
    read_idx_ = (read_idx_ + 1) & N_;
    node->busy.store(false, std::memory_order_release);
  }

  T consume()
  {
    Node* node = &data_[read_idx_];
    while (!node->busy.load(std::memory_order_acquire))
      ;
    std::atomic_thread_fence(std::memory_order_acquire);
    T val = *std::launder(reinterpret_cast<T*>(node->paylod));
    std::atomic_thread_fence(std::memory_order_acquire);
    read_idx_ = (read_idx_ + 1) & N_;
    node->busy.store(false, std::memory_order_release);
    return val;
  }

  template <class... Args>
  void push(Args&&... args)
  {
    Node* node = &data_[write_idx_];
    while (node->busy.load(std::memory_order_acquire))
      ;

    ::new (node->paylod) T(std::forward<Args>(args)...);
    std::atomic_thread_fence(std::memory_order_release);
    write_idx_ = (write_idx_ + 1) & N_;
    std::atomic_thread_fence(std::memory_order_release);
    node->busy.store(true, std::memory_order_release);
  }
};

TEST_CASE("SPSC latency test")
{
  size_t constexpr BATCH_NUM = 2;
  using Queue = SPMCMulticastQueueReliable<int, ProducerKind::Unordered, 1, BATCH_NUM>;
  uint64_t N = 10000;

  Queue A_queue(128);
  ConsumerBlocking<Queue> A_consumer(A_queue);
  ProducerBlocking<Queue> A_producer(A_queue);
  A_queue.start();

  Queue B_queue(128);
  ConsumerBlocking<Queue> B_consumer(B_queue);
  ProducerBlocking<Queue> B_producer(B_queue);
  B_queue.start();

  volatile uint64_t elapsed_time_ns = 0;
  std::jthread B_thread(
    [N, &elapsed_time_ns, &A_consumer, &B_producer]()
    {
      int begin = A_consumer.consume();
      CHECK(begin == 0);
      // we are ready now to start the test!

      int i = 1;
      auto from = std::chrono::steady_clock::now();
      while (i <= N)
      {
        B_producer.emplace(1);
        A_consumer.consume([&](int v) mutable { i += v; });
      }

      auto diff = std::chrono::steady_clock::now() - from;
      elapsed_time_ns = std::chrono::nanoseconds(diff).count();
      B_producer.emplace(2); // mark the end
    });

  int i = 1;
  A_producer.emplace(0);
  while (i <= N)
  {
    B_consumer.consume([&](int v) mutable { i += v; });
    A_producer.emplace(1);
  }

  int end = B_consumer.consume();
  CHECK(end == 2);

  auto avg_roundtrip_latency = elapsed_time_ns / N;
  std::cout << " [SPSC] Total time is = " << elapsed_time_ns << "ns \n";
  std::cout << " [SPSC] Avg round-trip latency = " << avg_roundtrip_latency << "ns \n";
  CHECK(avg_roundtrip_latency < 200);
}

TEST_CASE("SPSC2 latency test")
{
  uint64_t N = 10000;
  SPSC2<int> A_queue(128);
  SPSC2<int> B_queue(128);

  volatile uint64_t elapsed_time_ns = 0;
  std::jthread B_thread(
    [N, &elapsed_time_ns, &A_queue, &B_queue]()
    {
      int begin = A_queue.consume();
      CHECK(begin == 0);
      // we are ready now to start the test!

      int i = 1;
      auto from = std::chrono::steady_clock::now();
      while (i <= N)
      {
        B_queue.push(1);
        A_queue.consume([&](int v) mutable { i += v; });
      }

      auto diff = std::chrono::steady_clock::now() - from;
      elapsed_time_ns = std::chrono::nanoseconds(diff).count();
      B_queue.push(2); // mark the end
    });

  int i = 1;
  A_queue.push(0);
  while (i <= N)
  {
    B_queue.consume([&](int v) mutable { i += v; });
    A_queue.push(1);
  }

  sleep(1);
  int end = B_queue.consume();
  CHECK(end == 2);

  auto avg_roundtrip_latency = elapsed_time_ns / N;
  TLOG << " [SPSC2] Total time is = " << elapsed_time_ns << "ns \n";
  TLOG << " [SPSC2] Avg round-trip latency = " << avg_roundtrip_latency << "ns \n";
  CHECK(avg_roundtrip_latency < 200);
}

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }