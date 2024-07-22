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
#include "detail/consumer.h"
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
    out << "id=" << o.id << " vol=" << o.vol << " price=" << o.price << " side=" << o.side << "\n";
    return out;
  }
};

int main()
{
  using Queue = SPMCMulticastQueueReliableBounded<Order, 1>;
  Queue q(8);

  constexpr bool blocking = true;
  ConsumerBlocking<Queue> c(q);
  ProducerBlocking<Queue> p(q);
  q.start();

  p.emplace(1u, 1u, 100.0, 'A');
  p.emplace(2u, 1u, 100.0, 'A');
  p.emplace(3u, 1u, 100.0, 'A');

  const_consumer_iterator<ConsumerBlocking<Queue>> it = c.cbegin();
  int i = 1;
  while (it != c.cend())
  {
    std::cout << *it;
    if (i++ == 3)
      q.stop(); // this would mark the end for the next increment
    ++it;
  }

  return 0;
}