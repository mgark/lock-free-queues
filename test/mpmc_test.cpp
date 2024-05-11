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
  using Queue = SPMCMulticastQueueReliable<Order, 2>;
  Queue q1(8);
  Queue q2(8);

  constexpr bool blocking = true;
  ConsumerBlocking<Queue> c1[2] = {q1, q2};
  ConsumerBlocking<Queue> c2[2] = {q1, q2};
  ProducerBlocking<Queue> p1(q1);
  ProducerBlocking<Queue> p2(q2);

  q1.start();
  q2.start();
  {
    auto r = p1.emplace(1u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
    r = p2.emplace(2u, 2u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }
  {
    auto r = c1[0].consume([](const Order& o) { std::cout << o; });
    CHECK(ConsumeReturnCode::Consumed == r);
    r = c2[0].consume([](const Order& o) { std::cout << o; });
    CHECK(ConsumeReturnCode::Consumed == r);
    r = c2[1].consume([](const Order& o) { std::cout << o; });
    CHECK(ConsumeReturnCode::Consumed == r);
    r = c1[1].consume([](const Order& o) { std::cout << o; });
    CHECK(ConsumeReturnCode::Consumed == r);
  }
}

TEST_CASE("MPMC Anycasy Blocking consumer functional test")
{
  using Queue = SPMCMulticastQueueReliable<Order, 2>;
  Queue q1(8);
  Queue q2(8);

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

  {
    q1.stop();
    q2.stop();
    alignas(Order) std::byte storage[sizeof(Order)];
    auto r = c2.consume_raw(static_cast<void*>(storage));
    CHECK(ConsumeReturnCode::Stopped == r);
  }
}

TEST_CASE("MPMC Anycasy Non-Blocking consumer functional test")
{
  using Queue = SPMCMulticastQueueReliable<Order, 2>;
  Queue q1(8);
  Queue q2(8);

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

  {
    q1.stop();
    q2.stop();
    alignas(Order) std::byte storage[sizeof(Order)];
    auto r = c2.consume_raw(static_cast<void*>(storage));
    CHECK(ConsumeReturnCode::Stopped == r);
  }
}

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }