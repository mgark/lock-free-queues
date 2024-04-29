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
  using Queue = SPMCBoundedQueue<Order, ProducerKind::Unordered, 2>;
  Queue q(8);

  constexpr bool blocking = true;
  Consumer<Queue, blocking> c1(q);
  Consumer<Queue, blocking> c2(q);
  Producer<Queue, blocking> p(q);
  {
    auto r = p.emplace(1u, 1u, 100.0, 'A');
    assert(ProducerReturnCode::Published == r);
  }
  {
    auto r = c1.consume([&q](const Order& o) mutable { std::cout << o; });
    assert(ConsumerReturnCode::Consumed == r);
    r = c2.consume([&q](const Order& o) mutable { std::cout << o; });
    assert(ConsumerReturnCode::Consumed == r);
  }

  return 0;
}