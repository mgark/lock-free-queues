#pragma once

#include <iostream>
#include <mutex>

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