#include <benchmark/benchmark.h>

static void test_test(benchmark::State &state) {
  for (auto _ : state) {
    for (std::size_t i = 0; i < 1000; ++i) {
      double x = 1.000;
      benchmark::DoNotOptimize(x / 2.0);
    }
  }
}

BENCHMARK(test_test)->Iterations(1000);
BENCHMARK_MAIN();
