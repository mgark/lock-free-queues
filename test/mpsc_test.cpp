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

TEST_CASE("MPSC functional test")
{
  using Queue = SPMCMulticastQueueReliableBounded<Order, 2, 2>;
  Queue q1(16);
  Queue q2(16);

  ConsumerBlocking<Queue> c1[2] = {q1, q2};
  ProducerBlocking<Queue> p1;
  ProducerBlocking<Queue> p2;
  p1.attach(q1);
  p2.attach(q2);
  {
    auto r = p1.emplace(1u, 1u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
    r = p2.emplace(2u, 2u, 100.0, 'A');
    CHECK(ProduceReturnCode::Published == r);
  }

  auto it1 = c1[0].cbegin();
  auto it2 = c1[1].cbegin();
  {
    CHECK(1u == it1->id);
    CHECK(2u == it2->id);
  }
}

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }