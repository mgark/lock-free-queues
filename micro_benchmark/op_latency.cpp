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

#include <atomic>
#include <benchmark/benchmark.h>
#include <emmintrin.h>

static void cpu_pause_latency(benchmark::State& state)
{
  for (auto _ : state)
  {
    benchmark::DoNotOptimize((_mm_pause(), 0));
  }
}

static void size_t_div_new_val(benchmark::State& state)
{
  size_t val = 123123123;
  size_t i = 512;
  volatile size_t res = 512;
  for (auto _ : state)
  {
    res = val / res;
    ++val;
  }
}

static void size_t_div_constant(benchmark::State& state)
{
  size_t val = 123123123;
  volatile size_t res = 512;
  for (auto _ : state)
  {
    res = val / 512;
    ++val;
  }
}

static void fetch_add_latency(benchmark::State& state)
{
  static std::atomic_size_t counter{};
  for (auto _ : state)
  {
    benchmark::DoNotOptimize(counter.fetch_add(1, std::memory_order_acq_rel));
  }
}

static void cas_latency(benchmark::State& state)
{
  static std::atomic_size_t counter{0};
  size_t tmp;
  for (auto _ : state)
  {
    benchmark::DoNotOptimize(counter.compare_exchange_strong(tmp, 0, std::memory_order_acq_rel));
  }
}
BENCHMARK(cpu_pause_latency)->Iterations(100000);
BENCHMARK(size_t_div_new_val)->Iterations(100000);
BENCHMARK(size_t_div_constant)->Iterations(100000);
BENCHMARK(fetch_add_latency)->Threads(2)->Iterations(1000000);
BENCHMARK(cas_latency)->Threads(2)->Iterations(1000000);
BENCHMARK_MAIN();
