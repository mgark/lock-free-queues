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
  using Queue = SPMCBoundedQueue<Order, ProducerKind::Sequential, 2>;
  Queue q1(8);
  Queue q2(8);

  constexpr bool blocking = true;
  Consumer<Queue, blocking> c1[2] = {q1, q2};
  Producer<Queue, blocking> p1(q1);
  Producer<Queue, blocking> p2(q2);
  {
    auto r = p1.emplace(1u, 1u, 100.0, 'A');
    assert(ProducerReturnCode::Published == r);
    r = p2.emplace(2u, 2u, 100.0, 'A');
    assert(ProducerReturnCode::Published == r);
  }
  {
    auto r = c1[0].consume([](const Order& o) { std::cout << o; });
    assert(ConsumerReturnCode::Consumed == r);
    r = c1[1].consume([](const Order& o) { std::cout << o; });
    assert(ConsumerReturnCode::Consumed == r);
  }

  return 0;
}