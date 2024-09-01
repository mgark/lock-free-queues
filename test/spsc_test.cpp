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
#include "detail/common.h"
#include "detail/single_bit_reuse.h"
#include <assert.h>
#include <catch2/catch_all.hpp>
#include <cstdint>
#include <mpmc.h>
#include <thread>

TEST_CASE("SPSC basic functional test - version based queue")
{
  using Queue = SPMCMulticastQueueReliableBounded<Order, 1>;
  Queue q(8);

  constexpr bool blocking = true;
  ConsumerBlocking<Queue> c(q);
  ProducerBlocking<Queue> p(q);
  q.start();

  {
    auto r = p.emplace(1u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }

  {
    auto r = c.consume([&q](const Order& o) mutable { CHECK(o.id == 1u); });
    CHECK(ConsumeReturnCode::Consumed == r);
  }
}

TEST_CASE("SPSC basic functional test - bit-reuse based queue")
{
  using MsgType = integral_msb_always_0<uint32_t>;
  using Queue = SPMCMulticastQueueReliableBounded<MsgType, 1, 1, 2>;
  Queue q(2);

  static_assert(msb_always_0<MsgType>, "asdf");

  ConsumerNonBlocking<Queue> c(q);
  ProducerNonBlocking<Queue> p(q);
  q.start();

  {
    auto r = p.emplace(1u);
    CHECK(ProduceReturnCode::Published == r);
  }
  {
    auto r = p.emplace(2u);
    CHECK(ProduceReturnCode::Published == r);
  }
  {
    auto r = p.emplace(3u);
    CHECK(ProduceReturnCode::SlowConsumer == r);
  }

  {
    auto r = c.consume([&q](const MsgType& o) mutable { CHECK(o == MsgType::type{1u}); });
    CHECK(ConsumeReturnCode::Consumed == r);
  }
  {
    auto r = p.emplace(4u);
    CHECK(ProduceReturnCode::Published == r);
  }
  {
    auto r = c.consume([&q](const MsgType& o) mutable { CHECK(o == MsgType::type{2u}); });
    CHECK(ConsumeReturnCode::Consumed == r);
  }
  {
    auto r = c.consume([&q](const MsgType& o) mutable { CHECK(o == MsgType::type{4u}); });
    CHECK(ConsumeReturnCode::Consumed == r);
  }
}

TEST_CASE("SPSC stop / start test")
{
  // test stop and stating the queue few times
  using Queue = SPMCMulticastQueueReliableBounded<Order, 1>;
  Queue q(8);

  constexpr bool blocking = true;
  ConsumerBlocking<Queue> c(q);
  ProducerBlocking<Queue> p(q);
  q.start();

  {
    auto r = p.emplace(1u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }

  {
    auto r = c.consume([&q](const Order& o) mutable { std::cout << o; });
    CHECK(ConsumeReturnCode::Consumed == r);
  }

  std::jthread t(
    [&]()
    {
      sleep(1); /*wait until consumer calls consume function below*/
      q.stop();
    });
  t.join();
  {
    auto cr = c.consume([&q](const Order& o) mutable { std::cout << o; });
    CHECK(ConsumeReturnCode::Stopped == cr);

    auto pr = p.emplace(1u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::NotRunning == pr);
  }

  q.start();
  {
    auto pr = p.emplace(1u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == pr);

    auto cr = c.consume([&q](const Order& o) mutable { std::cout << o; });
    CHECK(ConsumeReturnCode::Consumed == cr);
  }
}

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }