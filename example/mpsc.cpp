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

#include "detail/common.h"
#include <assert.h>
#include <mpmc.h>

struct Order
{
  size_t id;
  size_t vol;
  double price;
  char side;

  friend std::ostream& operator<<(std::ostream& out, const Order& o)
  {
    out << "\n id=" << o.id << " vol=" << o.vol << " price=" << o.price << " side=" << o.side << "\n";
    return out;
  }
};

int main()
{
  using Queue = SPMCMulticastQueueReliableBounded<Order, 2>;
  Queue q1(8);
  Queue q2(8);

  constexpr bool blocking = true;
  ConsumerBlocking<Queue> c1[2] = {q1, q2};
  ProducerBlocking<Queue> p1(q1);
  ProducerBlocking<Queue> p2(q2);
  q1.start();
  q2.start();
  {
    p1.emplace(1u, 1u, 100.0, 'A');
    p2.emplace(2u, 2u, 100.0, 'A');
  }
  {
    c1[0].consume([](const Order& o) { std::cout << o; });
    c1[1].consume([](const Order& o) { std::cout << o; });
  }

  return 0;
}