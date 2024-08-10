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

#include "common_test_utils.h"
#include "detail/common.h"
#include <assert.h>
#include <catch2/catch_all.hpp>
#include <mpmc.h>
#include <thread>

TEST_CASE("Single 0 bit bit masks test - uint8_t")
{
  auto uint8_masks = generate_bit_masks_with_single_0_bit<std::uint8_t>();

  CHECK(uint8_masks[0] == 0b01111111);
  CHECK(uint8_masks[1] == 0b10111111);
  CHECK(uint8_masks[2] == 0b11011111);
  CHECK(uint8_masks[3] == 0b11101111);
  CHECK(uint8_masks[4] == 0b11110111);
  CHECK(uint8_masks[5] == 0b11111011);
  CHECK(uint8_masks[6] == 0b11111101);
  CHECK(uint8_masks[7] == 0b11111110);
}

TEST_CASE("Single 0 bit bit masks test - int")
{
  auto int_masks = generate_bit_masks_with_single_0_bit<int>();

  CHECK(int_masks[0] == 0b01111111111111111111111111111111);
  CHECK(int_masks[1] == 0b10111111111111111111111111111111);
  CHECK(int_masks[31] == 0b11111111111111111111111111111110);
  CHECK(int_masks[30] == 0b11111111111111111111111111111101);
  CHECK(int_masks[24] == 0b11111111111111111111111101111111);
}

TEST_CASE("Single 1 bit bit masks test - uint8_t")
{
  auto uint8_masks = generate_bit_masks_with_single_1_bit<std::uint8_t>();

  CHECK(uint8_masks[0] == 0b10000000);
  CHECK(uint8_masks[1] == 0b01000000);
}

TEST_CASE("Single 1 bit bit masks test - int")
{
  auto int_masks = generate_bit_masks_with_single_1_bit<int>();

  CHECK(int_masks[0] == 0b10000000000000000000000000000000);
  CHECK(int_masks[31] == 0b00000000000000000000000000000001);
}

int main(int argc, char** argv) { return Catch::Session().run(argc, argv); }