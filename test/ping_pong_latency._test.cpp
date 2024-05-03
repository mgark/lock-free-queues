#include "common_test_utils.h"
#include <assert.h>
#include <catch2/catch_all.hpp>
#include <mpmc.h>

TEST_CASE("SPSC latency test") {}

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }