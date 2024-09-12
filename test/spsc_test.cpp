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

  {
    auto r = p.emplace(1u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }

  auto it = c.cbegin();
  {
    auto r = *it;
    CHECK(r.id == 1u);
  }

  {
    auto r = p.emplace(3u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }
  {
    auto r = *(++it);
    CHECK(r.id == 3u);
  }
}

TEST_CASE("SPSC basic functional test - wrap around the queue")
{
  using MsgType = uint32_t;
  using Queue = SPMCMulticastQueueReliableBounded<MsgType, 1, 1, 2>;
  size_t N = 1024;
  Queue q(N);

  ConsumerNonBlocking<Queue> c(q);
  ProducerNonBlocking<Queue> p(q);

  for (size_t i = 1; i <= N; ++i)
  {
    auto r = p.emplace(i);
    CHECK(ProduceReturnCode::Published == r);
  }

  {
    auto r = p.emplace(3u);
    CHECK(ProduceReturnCode::SlowConsumer == r);
  }

  auto it = c.cbegin();
  for (size_t i = 1; i <= N; ++i, ++it)
  {
    CHECK(*it == MsgType{static_cast<MsgType>(i)});
  }
}

TEST_CASE("SPSC stop / start test")
{
  // test stop and stating the queue few times
  using Queue = SPMCMulticastQueueReliableBounded<Order, 1>;
  Queue q(16);

  constexpr bool blocking = true;
  ConsumerBlocking<Queue> c(q);
  ProducerBlocking<Queue> p(q);

  {
    auto r = p.emplace(1u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }

  auto it = c.cbegin();
  CHECK(it->id == 1u);

  p.halt();
  CHECK(p.is_halted() == true);

  p.detach();
  p.attach(q);
  {
    auto pr = p.emplace(2u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == pr);

    ++it;
    CHECK(it->id == 2u);
  }
}

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }