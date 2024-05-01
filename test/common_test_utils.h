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

#pragma once

#include <iostream>
#include <mutex>
#include <type_traits>

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

struct OrderNonTrivial
{
  size_t id;
  size_t vol;
  double price;
  char side;

  ~OrderNonTrivial() {}

  friend std::ostream& operator<<(std::ostream& out, const OrderNonTrivial& o)
  {
    out << "id=" << o.id << " vol=" << o.vol << " price=" << o.price << " side=" << o.side << "\n";
    return out;
  }
};

static_assert(!std::is_trivial_v<OrderNonTrivial>);

class ConsoleLogger
{
public:
  ConsoleLogger() { guard().lock(); }
  ~ConsoleLogger() { guard().unlock(); }

  static std::mutex& guard()
  {
    static std::mutex g;
    return g;
  }

  ConsoleLogger& operator<<(const auto& v)
  {
    std::cout << v;
    return *this;
  }
};

#define TLOG ConsoleLogger()