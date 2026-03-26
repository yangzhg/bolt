/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 * --------------------------------------------------------------------------
 */

#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class VersionCompareTest : public SparkFunctionBaseTest {
 protected:
  std::optional<int32_t> versionCompare(
      std::optional<std::string> left,
      std::optional<std::string> right) {
    return evaluateOnce<int32_t>("version_compare(c0, c1)", left, right);
  }

  VectorPtr versionCompare(
      const std::vector<std::optional<std::string>>& left,
      const std::vector<std::optional<std::string>>& right) {
    return evaluate<SimpleVector<int32_t>>(
        "version_compare(c0, c1)",
        makeRowVector(
            {makeNullableFlatVector(left), makeNullableFlatVector(right)}));
  }
};

TEST_F(VersionCompareTest, javaTestCoverage) {
  EXPECT_EQ(versionCompare("1.1.2", "1.1"), 1);
  EXPECT_EQ(versionCompare("1.0", "1.1.3"), -1);
}

TEST_F(VersionCompareTest, nullAndBlankInputs) {
  EXPECT_EQ(versionCompare(std::nullopt, "1.0"), std::nullopt);
  EXPECT_EQ(versionCompare("1.0", std::nullopt), std::nullopt);
  EXPECT_EQ(versionCompare("", ""), 0);
  EXPECT_EQ(versionCompare(" ", ""), 0);
  EXPECT_EQ(versionCompare(" 1.02 ", "1.2"), 0);
  EXPECT_EQ(versionCompare("1.0.0", "1"), 0);
}

TEST_F(VersionCompareTest, delimiterSemanticsMatchJava) {
  EXPECT_EQ(versionCompare("1..2", "1.0.2"), 1);
  EXPECT_EQ(versionCompare(".1", "0.1"), 1);
  EXPECT_EQ(versionCompare("1.", "1"), 0);
  EXPECT_EQ(versionCompare("1.2.3", "1.2.3.0"), 0);
  EXPECT_EQ(versionCompare("1.2.3", "1.2.3.4"), -1);
}

TEST_F(VersionCompareTest, signedAndInvalidTokens) {
  EXPECT_EQ(versionCompare("-1.2", "-1.3"), -1);
  EXPECT_EQ(versionCompare("1.2.-3", "1.2.-4"), 1);
  EXPECT_EQ(versionCompare("2147483648", "1"), std::nullopt);
  EXPECT_EQ(versionCompare("a", "1"), std::nullopt);
  EXPECT_EQ(versionCompare("1. 2", "1.2"), std::nullopt);
}

TEST_F(VersionCompareTest, reusesScratchBuffersLikeJava) {
  auto result = versionCompare({"1.2.3", "1.2"}, {"1.2.3.4", "1.2.0"});
  ::bytedance::bolt::test::assertEqualVectors(
      makeNullableFlatVector<int32_t>({-1, 1}), result);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
