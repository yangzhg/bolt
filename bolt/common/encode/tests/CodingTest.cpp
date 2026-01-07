/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>

#include "bolt/common/encode/Coding.h"
namespace bytedance::bolt::common {

class VarintTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(VarintTest, EncodeDecodeUInt64) {
  uint64_t values[] = {
      0, 1, 127, 128, 255, 256, 1024, 10000, 123456789, UINT64_MAX};
  for (uint64_t val : values) {
    char buffer[Varint::kMaxSize64] = {0};
    char* dest = buffer;
    Varint::encode(val, &dest);

    int expected_size = Varint::size(val);
    EXPECT_EQ(dest - buffer, expected_size);

    const char* src = buffer;
    uint64_t decoded = Varint::decode(&src, dest - buffer);
    EXPECT_EQ(val, decoded);
  }
}

TEST_F(VarintTest, EncodeDecodeInt32) {
  int32_t values[] = {
      0, 1, 127, 128, 255, 256, 1024, 10000, 123456789, INT32_MAX};
  for (int32_t val : values) {
    char buffer[Varint::kMaxSize32] = {0};
    char* dest = buffer;
    Varint::encode(val, &dest);

    int expected_size = Varint::size(val);
    EXPECT_EQ(dest - buffer, expected_size);

    const char* src = buffer;
    int32_t decoded = Varint::decode(&src, dest - buffer);
    EXPECT_EQ(val, decoded);
  }
}

TEST_F(VarintTest, EncodeDecodeUInt128) {
  UInt128 values[] = {
      UInt128(0, 0),
      UInt128(0, 1),
      UInt128(0, 127),
      UInt128(0, 128),
      UInt128(0, UINT64_MAX),
      UInt128(1, 0),
      UInt128(UINT64_MAX, UINT64_MAX)};
  for (UInt128 val : values) {
    char buffer[Varint::kMaxSize128] = {0};
    char* dest = buffer;
    Varint::encode128(val, &dest);

    int expected_size = Varint::size128(val);
    EXPECT_EQ(dest - buffer, expected_size);

    const char* src = buffer;
    UInt128 decoded = Varint::decode128(&src, dest - buffer);
    EXPECT_EQ(val, decoded);
  }
}

TEST_F(VarintTest, SizeUInt64) {
  EXPECT_EQ(Varint::size(0), 1);
  EXPECT_EQ(Varint::size(1), 1);
  EXPECT_EQ(Varint::size(127), 1);
  EXPECT_EQ(Varint::size(128), 2);
  EXPECT_EQ(Varint::size(UINT64_MAX), 10);
}

TEST_F(VarintTest, SizeUInt128) {
  EXPECT_EQ(Varint::size128(UInt128(0, 0)), 1);
  EXPECT_EQ(Varint::size128(UInt128(0, 1)), 1);
  EXPECT_EQ(Varint::size128(UInt128(0, 127)), 1);
  EXPECT_EQ(Varint::size128(UInt128(0, 128)), 2);
  EXPECT_EQ(Varint::size128(UInt128(1, 0)), 10);
  EXPECT_EQ(Varint::size128(UInt128(UINT64_MAX, UINT64_MAX)), 19);
}

TEST_F(VarintTest, EncodeDecodeEdgeCases) {
  // 测试边界情况
  uint64_t edge_values[] = {127, 128, 255, 256, 1023, 1024};
  for (uint64_t val : edge_values) {
    char buffer[Varint::kMaxSize64] = {0};
    char* dest = buffer;
    Varint::encode(val, &dest);
    int expected_size = Varint::size(val);
    EXPECT_EQ(dest - buffer, expected_size);
    const char* src = buffer;
    uint64_t decoded = Varint::decode(&src, dest - buffer);
    EXPECT_EQ(val, decoded);
  }
}

TEST_F(VarintTest, EncodeDecode128EdgeCases) {
  // 测试128位整数的边界情况
  UInt128 edge_values[] = {
      UInt128(0, 127),
      UInt128(0, 128),
      UInt128(0, 255),
      UInt128(0, 256),
      UInt128(1, 0),
      UInt128(UINT64_MAX, UINT64_MAX)};
  for (UInt128 val : edge_values) {
    char buffer[Varint::kMaxSize128] = {0};
    char* dest = buffer;
    Varint::encode128(val, &dest);
    int expected_size = Varint::size128(val);
    EXPECT_EQ(dest - buffer, expected_size);
    const char* src = buffer;
    UInt128 decoded = Varint::decode128(&src, dest - buffer);
    EXPECT_EQ(val, decoded);
  }
}
} // namespace bytedance::bolt::common
