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
#include "detail/consumer.h"
#include <assert.h>
#include <catch2/catch_all.hpp>
#include <mpmc.h>
#include <thread>

TEST_CASE("SPSC consumer iterator functional test")
{
  SECTION("Blocking consumer")
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

      r = p.emplace(2u, 1u, 100.0, 'A');
      CHECK(ProduceReturnCode::Published == r);

      r = p.emplace(3u, 1u, 100.0, 'A');
      CHECK(ProduceReturnCode::Published == r);
    }

    {
      int i = 1;
      auto it = c.cbegin();
      while (it != c.cend() && it->id != 3)
      {
        int v = it->id;
        CHECK(v == i);
        ++i;
        ++it;
      }
    }
  }

  SECTION("Non-Blocking consumer")
  {
    using Queue = SPMCMulticastQueueReliableBounded<Order, 1>;
    Queue q(8);

    constexpr bool blocking = true;
    ConsumerNonBlocking<Queue> c(q);
    ProducerBlocking<Queue> p(q);
    q.start();

    {
      auto r = p.emplace(1u, 1u, 100.0, 'A');
      CHECK(ProduceReturnCode::Published == r);

      r = p.emplace(2u, 1u, 100.0, 'A');
      CHECK(ProduceReturnCode::Published == r);

      r = p.emplace(3u, 1u, 100.0, 'A');
      CHECK(ProduceReturnCode::Published == r);
    }

    {
      int i = 1;
      auto it = c.cbegin();
      while (it != c.cend())
      {
        int v = it->id;
        CHECK(v == i++);
        ++it;
      }
    }
  }
}

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }