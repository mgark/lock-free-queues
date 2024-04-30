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

#include "common_test.h"
#include <assert.h>
#include <catch2/catch_all.hpp>
#include <mpmc.h>

TEST_CASE("SPSC functional test")
{
  using Queue = SPMCBoundedQueue<Order, ProducerKind::Unordered, 1>;
  Queue q(8);

  constexpr bool blocking = true;
  Consumer<Queue, blocking> c(q);
  Producer<Queue, blocking> p(q);

  {
    auto r = p.emplace(1u, 1u, 100.0, 'A');
    CHECK(ProducerReturnCode::Published == r);
  }

  {
    auto r = c.consume([&q](const Order& o) mutable { std::cout << o; });
    CHECK(ConsumerReturnCode::Consumed == r);
  }
}

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }