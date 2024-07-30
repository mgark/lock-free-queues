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
#include "detail/consumer.h"
#include <assert.h>
#include <catch2/catch_all.hpp>
#include <mpmc.h>

#ifndef _DISABLE_UNRELIABLE_MULTICAST_TEST_
TEST_CASE("SPMC Conflated basic functional test - blocking producer and consumer")
{
  using Queue = SPMCMulticastQueueUnreliable<Order, 2, 2>;
  Queue q(2);

  constexpr bool blocking = true;
  ConsumerBlocking<Queue> c1;
  ConsumerBlocking<Queue> c2;
  ProducerBlocking<Queue> p(q);
  CHECK(ConsumerAttachReturnCode::Attached == c1.attach(q));
  CHECK(ConsumerAttachReturnCode::Attached == c2.attach(q));
  q.start();

  {
    // queue has max 2 times so that first two items would be overriden by subsequent two published values
    for (size_t i = 1; i <= 4; ++i)
    {
      CHECK(ProduceReturnCode::Published == p.emplace(i, i, 100.0, 'A'));
    }
  }

  {
    for (size_t i = 3; i <= 4; ++i)
    {
      CHECK(ConsumeReturnCode::Consumed ==
            c1.consume([&q, i](const Order& o) mutable { CHECK(o.id == i); }));
      CHECK(ConsumeReturnCode::Consumed ==
            c2.consume([&q, i](const Order& o) mutable { CHECK(o.id == i); }));
    }
  }
}

TEST_CASE("SPMC Conflated basic functional test - NON blocking producer and consumer")
{
  using Queue = SPMCMulticastQueueUnreliable<Order, 2, 2>;
  Queue q(2);

  constexpr bool blocking = true;
  ConsumerNonBlocking<Queue> c1;
  ConsumerNonBlocking<Queue> c2;
  ProducerNonBlocking<Queue> p(q);
  CHECK(ConsumerAttachReturnCode::Attached == c1.attach(q));
  CHECK(ConsumerAttachReturnCode::Attached == c2.attach(q));
  q.start();

  {
    for (size_t i = 1; i <= 4; ++i)
    {
      CHECK(ProduceReturnCode::Published == p.emplace(i, i, 100.0, 'A'));
    }
  }

  {
    for (size_t i = 3; i <= 4; ++i)
    {
      CHECK(ConsumeReturnCode::Consumed ==
            c1.consume([&q, i](const Order& o) mutable { CHECK(o.id == i); }));
      CHECK(ConsumeReturnCode::Consumed ==
            c2.consume([&q, i](const Order& o) mutable { CHECK(o.id == i); }));
    }
  }
}
#endif

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }