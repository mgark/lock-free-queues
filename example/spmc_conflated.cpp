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
    out << "id=" << o.id << " vol=" << o.vol << " price=" << o.price << " side=" << o.side << "\n";
    return out;
  }
};

int main()
{
#if defined(__x86_64__)
  constexpr size_t MAX_CONSUMER_NUM = 2;
  constexpr size_t BATCH_NUM = 1;
  using Queue = SPMCBoundedConflatedQueue<Order, ProducerKind::Unordered, MAX_CONSUMER_NUM, BATCH_NUM>;
  Queue q(2); // only hold 2 slots in the ring buffer

  constexpr bool blocking = true;
  Consumer<Queue, blocking> c1(q);
  Consumer<Queue, blocking> c2(q);
  Producer<Queue, blocking> p(q);
  {
    p.emplace(1u, 1u, 100.0, 'A');
    p.emplace(2u, 2u, 100.0, 'A');
    p.emplace(3u, 3u, 99.5, 'B');
  }

  {
    // first message created by the producer would be discared and consumers would only see last two due to conflation
    c1.consume([&q](const Order& o) mutable { std::cout << o; });
    c1.consume([&q](const Order& o) mutable { std::cout << o; });
    c2.consume([&q](const Order& o) mutable { std::cout << o; });
    c2.consume([&q](const Order& o) mutable { std::cout << o; });
  }
#endif

  return 0;
}