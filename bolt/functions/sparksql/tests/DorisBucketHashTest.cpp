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

#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

#include <limits>

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class DorisBucketHashTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  int64_t dorisBucketHash(std::optional<T> arg) {
    return evaluateOnce<int64_t>("doris_bucket_hash(c0)", arg).value();
  }
};

TEST_F(DorisBucketHashTest, bigint) {
  using TestCase = std::pair<std::optional<int64_t>, int64_t>;
  const std::vector<TestCase> testCases = {
      {1850844824426545LL, 1619943111LL},
      {7473047905566392337LL, 4101688503LL},
      {7215918035683524610LL, 3926960695LL},
      {7281650783374802946LL, 78375666LL},
      {7504905681150050312LL, 3753993746LL},
      {7518039680018186256LL, 998734313LL},
      {7419950859230150657LL, 53994132LL},
      {7538463130418970641LL, 4217641944LL},
      {7529175655553351681LL, 825021984LL},
      {7447764307099631632LL, 1721364636LL},
      {7332052291845079042LL, 2748414312LL},
      {std::nullopt, 558161692LL},
  };

  for (const auto& [input, expected] : testCases) {
    EXPECT_EQ(expected, dorisBucketHash(input));
  }
}

TEST_F(DorisBucketHashTest, integer) {
  using TestCase = std::pair<std::optional<int32_t>, int64_t>;
  const std::vector<TestCase> testCases = {
      {1, 2583214201LL},
      {2, 2337085335LL},
      {3, 871461106LL},
      {4, 2921744459LL},
      {5, 379203374LL},
      {std::nullopt, 558161692LL},
  };

  for (const auto& [input, expected] : testCases) {
    EXPECT_EQ(expected, dorisBucketHash(input));
  }
}

TEST_F(DorisBucketHashTest, integerBoundaries) {
  EXPECT_EQ(558161692LL, dorisBucketHash(std::optional<int32_t>{0}));
  EXPECT_EQ(4294967295LL, dorisBucketHash(std::optional<int32_t>{-1}));
  EXPECT_EQ(
      3439090748LL,
      dorisBucketHash(
          std::optional<int32_t>{std::numeric_limits<int32_t>::min()}));
  EXPECT_EQ(
      306674911LL,
      dorisBucketHash(
          std::optional<int32_t>{std::numeric_limits<int32_t>::max()}));
}

TEST_F(DorisBucketHashTest, bigintBoundaries) {
  EXPECT_EQ(1696784233LL, dorisBucketHash(std::optional<int64_t>{0}));
  EXPECT_EQ(558161692LL, dorisBucketHash(std::optional<int64_t>{-1}));
  EXPECT_EQ(
      2291817545LL,
      dorisBucketHash(
          std::optional<int64_t>{std::numeric_limits<int64_t>::min()}));
  EXPECT_EQ(
      3439090748LL,
      dorisBucketHash(
          std::optional<int64_t>{std::numeric_limits<int64_t>::max()}));
}

TEST_F(DorisBucketHashTest, real) {
  using TestCase = std::pair<std::optional<float>, int64_t>;
  const std::vector<TestCase> testCases = {
      {1.23f, 3308642420LL},
      {2.3461231f, 196516026LL},
      {3.144351f, 273064891LL},
      {4.15464576f, 3535102556LL},
      {5.45745746f, 935476093LL},
      {std::nullopt, 558161692LL},
  };

  for (const auto& [input, expected] : testCases) {
    EXPECT_EQ(expected, dorisBucketHash(input));
  }
}

TEST_F(DorisBucketHashTest, realSpecialValues) {
  EXPECT_EQ(558161692LL, dorisBucketHash(std::optional<float>{0.0f}));
  EXPECT_EQ(3439090748LL, dorisBucketHash(std::optional<float>{-0.0f}));
  EXPECT_EQ(
      704931071LL,
      dorisBucketHash(
          std::optional<float>{std::numeric_limits<float>::quiet_NaN()}));
  EXPECT_EQ(
      3665636346LL,
      dorisBucketHash(
          std::optional<float>{std::numeric_limits<float>::infinity()}));
  EXPECT_EQ(
      935700698LL,
      dorisBucketHash(
          std::optional<float>{-std::numeric_limits<float>::infinity()}));
}

TEST_F(DorisBucketHashTest, doublePrecision) {
  using TestCase = std::pair<std::optional<double>, int64_t>;
  const std::vector<TestCase> testCases = {
      {1.23, 2958655112LL},
      {2.3461231, 4119372356LL},
      {3.144351, 1189811858LL},
      {4.15464576, 2188937371LL},
      {5.45745746, 134171275LL},
      {std::nullopt, 558161692LL},
  };

  for (const auto& [input, expected] : testCases) {
    EXPECT_EQ(expected, dorisBucketHash(input));
  }
}

TEST_F(DorisBucketHashTest, doubleSpecialValues) {
  EXPECT_EQ(1696784233LL, dorisBucketHash(std::optional<double>{0.0}));
  EXPECT_EQ(2291817545LL, dorisBucketHash(std::optional<double>{-0.0}));
  EXPECT_EQ(
      2046679153LL,
      dorisBucketHash(
          std::optional<double>{std::numeric_limits<double>::quiet_NaN()}));
  EXPECT_EQ(
      2971947641LL,
      dorisBucketHash(
          std::optional<double>{std::numeric_limits<double>::infinity()}));
  EXPECT_EQ(
      1553781081LL,
      dorisBucketHash(
          std::optional<double>{-std::numeric_limits<double>::infinity()}));
}

TEST_F(DorisBucketHashTest, varchar) {
  EXPECT_EQ(558161692LL, dorisBucketHash<std::string>(std::nullopt));
  EXPECT_EQ(0LL, dorisBucketHash(std::optional<std::string>{""}));
  EXPECT_EQ(1557323817LL, dorisBucketHash(std::optional<std::string>{"Spark"}));
  EXPECT_EQ(3457936166LL, dorisBucketHash(std::optional<std::string>{"你好"}));
  EXPECT_EQ(903183767LL, dorisBucketHash(std::optional<std::string>{"😄"}));
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
