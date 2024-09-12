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

#include "common_test_utils.h"
#include <assert.h>
#include <catch2/catch_all.hpp>
#include <mpmc.h>

TEST_CASE("MPMC functional test")
{
  using Queue = SPMCMulticastQueueReliableBounded<Order, 2, 2>;
  Queue q1(32);
  Queue q2(32);

  constexpr bool blocking = true;
  ConsumerBlocking<Queue> c1[2] = {q1, q2};
  ConsumerBlocking<Queue> c2[2] = {q1, q2};
  ProducerBlocking<Queue> p1(q1);
  ProducerBlocking<Queue> p2(q2);

  {
    auto r = p1.emplace(1u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
    r = p2.emplace(2u, 2u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }

  auto itc10 = c1[0].cbegin();
  auto itc20 = c2[0].cbegin();
  auto itc11 = c1[1].cbegin();
  auto itc21 = c2[1].cbegin();
  {
    CHECK(1u == itc10->id);
    CHECK(1u == itc20->id);
    CHECK(2u == itc11->id);
    CHECK(2u == itc21->id);
  }
}

#ifndef _DISABLE_SYNTHETIC_ANYCAST_TEST_
TEST_CASE("MPMC Anycasy Blocking consumer functional test")
{
  using Queue = SPMCMulticastQueueReliableBounded<Order, 2, 2>;
  Queue q1(32);
  Queue q2(32);

  ProducerBlocking<Queue> p1(q1);
  ProducerBlocking<Queue> p2(q2);

  AnycastConsumerGroup<Queue> consumer_group({&q1, &q2});
  AnycastConsumerBlocking<Queue> c1(consumer_group);
  AnycastConsumerBlocking<Queue> c2(consumer_group);

  q1.start();
  q2.start();

  {
    auto r = p1.emplace(1u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
    r = p2.emplace(2u, 2u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }
  {
    Order o;
    auto r = c1.consume(o);
    CHECK(ConsumeReturnCode::Consumed == r);
    CHECK(o.id == 1u);

    r = c2.consume(o);
    CHECK(ConsumeReturnCode::Consumed == r);
    CHECK(o.id == 2u);
  }

  {
    auto r = p1.emplace(3u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }

  {
    Order o = c1.consume();
    CHECK(o.id == 3u);
  }

  {
    auto r = p1.emplace(4u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }

  {
    Order o;
    alignas(Order) std::byte storage[sizeof(Order)];
    auto r = c1.consume_raw(static_cast<void*>(storage));
    CHECK(ConsumeReturnCode::Consumed == r);
    CHECK(reinterpret_cast<Order*>(storage)->id == 4u);
  }
}

TEST_CASE("MPMC Anycasy Non-Blocking consumer functional test")
{
  using Queue = SPMCMulticastQueueReliableBounded<Order, 2, 2>;
  Queue q1(32);
  Queue q2(32);

  ProducerBlocking<Queue> p1(q1);
  ProducerBlocking<Queue> p2(q2);

  AnycastConsumerGroup<Queue> consumer_group({&q1, &q2});
  AnycastConsumerNonBlocking<Queue> c1(consumer_group);
  AnycastConsumerNonBlocking<Queue> c2(consumer_group);

  q1.start();
  q2.start();

  {
    auto r = p1.emplace(1u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
    r = p2.emplace(2u, 2u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }
  {
    Order o;
    auto r = c1.consume(o);
    CHECK(ConsumeReturnCode::Consumed == r);
    CHECK(o.id == 1u);

    r = c2.consume(o);
    CHECK(ConsumeReturnCode::Consumed == r);
    CHECK(o.id == 2u);
  }

  {
    auto r = p1.emplace(3u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }

  {
    Order o = c1.consume();
    CHECK(o.id == 3u);
  }

  {
    auto r = p1.emplace(4u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }

  {
    Order o;
    alignas(Order) std::byte storage[sizeof(Order)];
    auto r = c1.consume_raw(static_cast<void*>(storage));
    CHECK(ConsumeReturnCode::Consumed == r);
    CHECK(reinterpret_cast<Order*>(storage)->id == 4u);
  }
}
#endif

TEST_CASE("Synchronized Blocking MPMC functional test")
{
  using Queue = SPMCMulticastQueueReliableBounded<Order, 2, 2>;
  Queue q1(32);

  constexpr bool blocking = true;
  ConsumerBlocking<Queue> c1{q1};
  ConsumerBlocking<Queue> c2{q1};

  ProducerBlocking<Queue> p1(q1);
  ProducerBlocking<Queue> p2(q1);

  {
    auto r = p1.emplace(1u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
    r = p2.emplace(2u, 2u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }
  auto it1 = c1.cbegin();
  auto it2 = c2.cbegin();
  {
    CHECK(1u == it1->id);
    CHECK(1u == it2->id);
    CHECK(2u == (++it1)->id);
    CHECK(2u == (++it2)->id);
  }
}

TEST_CASE("Synchronized Non-Blocking MPMC functional test")
{
  using Queue = SPMCMulticastQueueReliableBounded<Order, 2, 2, 2>;
  size_t N = 32;
  Queue q1(N);

  constexpr bool blocking = true;
  ConsumerNonBlocking<Queue> c1 = {q1};
  ConsumerNonBlocking<Queue> c2 = {q1};

  ProducerNonBlocking<Queue> p1(q1);
  ProducerNonBlocking<Queue> p2(q1);

  {
    for (size_t i = 0; i < N; ++i)
    {
      auto r = p1.emplace(i, 1u, 100.0, 'A');
      CHECK(ProduceReturnCode::Published == r);
    }

    auto r = p2.emplace(3u, 3u, 100.0, 'A');
    CHECK(ProduceReturnCode::SlowConsumer == r);
    r = p1.emplace(4u, 4u, 100.0, 'A');
    CHECK(ProduceReturnCode::SlowConsumer == r);
  }

  auto it1 = c1.cbegin();
  auto it2 = c2.cbegin();
  {
    for (size_t i = 0; i < N; ++i, ++it1)
      CHECK(i == it1->id);

    CHECK(it1 == c1.cend());

    for (size_t i = 0; i < N; ++i, ++it2)
      CHECK(i == it2->id);

    CHECK(it2 == c2.cend());
  }

  {
    auto r = p1.emplace(3u, 3u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
    r = p2.emplace(4u, 4u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }

  {
    // producer would continue publishing from the previous idx which failed before because the queue was full!
    CHECK(4 == c1.cbegin()->id);
    CHECK(4 == c2.cbegin()->id);
    CHECK(3 == (++c1.cbegin())->id);
    CHECK(3 == (++c2.cbegin())->id);
  }
}

TEST_CASE(
  "Synchronized Consumer and Producer Blocking MPMC functional test - non-multicast consumers")
{
  // this would mean that every message on the queue would be consumed only
  // once by one of the consumers
  constexpr bool _MULTICAST_ = false;

  constexpr bool blocking = true;
  size_t N = 32;
  using Queue = SPMCMulticastQueueReliableBounded<Order, 2, 2, 2, 0, _MULTICAST_>;
  Queue q1(N);

  ConsumerBlocking<Queue> c1 = {q1};
  ConsumerBlocking<Queue> c2 = {q1};

  ProducerBlocking<Queue> p1(q1);
  ProducerBlocking<Queue> p2(q1);

  {
    for (size_t i = 0; i < N; ++i)
    {
      auto r = p1.emplace(i, 1u, 100.0, 'A');
      CHECK(ProduceReturnCode::Published == r);
    }
  }

  auto it1 = c1.cbegin();
  {
    for (size_t i = 0; i < N; ++i, ++it1)
    {
      CHECK(i == it1->id);
      if (i == N - 1)
        break;
    }
  }

  {
    auto r = p1.emplace(4u, 4u, 'A');
    CHECK(ProduceReturnCode::Published == r);
    r = p2.emplace(3u, 3u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);

    CHECK((++it1)->id == 4);
    auto it2 = c2.cbegin();
    CHECK(it2->id == 3);
  }
}

TEST_CASE("MPMC functional test - Blocking Peek and Skip")
{
  using MsgType = uint32_t;
  using Queue = SPMCMulticastQueueReliableBounded<MsgType, 2, 2>;

  Queue q(1024);

  constexpr bool blocking = true;
  ConsumerBlocking<Queue> c1;
  ConsumerBlocking<Queue> c2;
  CHECK(ConsumerAttachReturnCode::Attached == c1.attach(q));
  CHECK(ConsumerAttachReturnCode::Attached == c2.attach(q));
  ProducerBlocking<Queue> p(q);

  {
    auto r = p.emplace(1u);
    CHECK(ProduceReturnCode::Published == r);
  }

  {
    const MsgType* val;
    val = c1.peek();
    CHECK(*val == 1u);
    c1.skip();
    val = c2.peek();
    CHECK(*val == 1u);
    c2.skip();
  }

  {
    auto r = p.emplace(2u);
    CHECK(ProduceReturnCode::Published == r);
  }

  {
    const MsgType* val;
    val = c1.peek();
    CHECK(*val == 2u);
    c1.skip();
    val = c2.peek();
    CHECK(*val == 2u);
    c2.skip();
  }
}
int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }