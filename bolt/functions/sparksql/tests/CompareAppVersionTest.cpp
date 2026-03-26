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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

#include <limits>

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class CompareAppVersionTest : public SparkFunctionBaseTest {
 protected:
  std::optional<int32_t> compareAppVersion(
      std::optional<std::string> lhs,
      std::optional<std::string> rhs) {
    return evaluateOnce<int32_t>("compare_app_version(c0, c1)", lhs, rhs);
  }

  VectorPtr compareAppVersion(
      const std::vector<std::optional<std::string>>& lhs,
      const std::vector<std::optional<std::string>>& rhs) {
    auto makeInputVector =
        [this](const std::vector<std::optional<std::string>>& input) {
          return makeFlatVector<StringView>(
              input.size(),
              [&](vector_size_t row) { return StringView(*input[row]); },
              [&](vector_size_t row) { return !input[row].has_value(); });
        };

    return evaluate(
        "compare_app_version(c0, c1)",
        makeRowVector({makeInputVector(lhs), makeInputVector(rhs)}));
  }
};

TEST_F(CompareAppVersionTest, migratedJavaTests) {
  EXPECT_EQ(compareAppVersion("1.2.3", "1.2.3"), 0);
  EXPECT_LT(compareAppVersion("1.2.3", "1.5.3").value(), 0);
  EXPECT_GT(compareAppVersion("10.1.2", "9.9.9").value(), 0);
}

TEST_F(CompareAppVersionTest, nullAndTrimHandling) {
  EXPECT_EQ(compareAppVersion(std::nullopt, "1.2.3"), std::nullopt);
  EXPECT_EQ(compareAppVersion("1.2.3", std::nullopt), std::nullopt);
  EXPECT_EQ(compareAppVersion(" 1.2.3 ", "1.2.3"), 0);
  EXPECT_EQ(compareAppVersion("", ""), 0);
  EXPECT_EQ(compareAppVersion(" ", ""), 0);
}

TEST_F(CompareAppVersionTest, generatedBehaviorCases) {
  EXPECT_EQ(compareAppVersion(".", "."), 0);
  EXPECT_EQ(compareAppVersion("1.", "1"), 0);
  EXPECT_EQ(compareAppVersion(".1", "0.1"), -1);
  EXPECT_EQ(compareAppVersion("1..2", "1.0.2"), -1);
  EXPECT_EQ(compareAppVersion("1.02", "1.2"), 0);
  EXPECT_EQ(compareAppVersion("1.2.3_b", "1.2.3_a"), 1);
  EXPECT_EQ(compareAppVersion("1.2.a", "1.2.0"), 49);
  EXPECT_EQ(
      compareAppVersion("1.2147483647", "1.-1"),
      std::numeric_limits<int32_t>::min());
  EXPECT_EQ(compareAppVersion("1.2147483648", "1.2"), 9);
  EXPECT_EQ(compareAppVersion("1.2", "1.2.0"), -1);
  EXPECT_EQ(compareAppVersion("1.2.0", "1.2"), 1);
  EXPECT_EQ(compareAppVersion("01.1", "1.1"), 0);
}

TEST_F(CompareAppVersionTest, mixedValidityBatch) {
  std::vector<std::optional<std::string>> lhs{
      "1.2.3", std::nullopt, "1.2.a", "1.2147483647", "1.2", " 1.2.3 "};
  std::vector<std::optional<std::string>> rhs{
      "1.2.3", "1.2.3", "1.2.0", "1.-1", "1.2.0", "1.2.3"};

  auto expected = makeNullableFlatVector<int32_t>(
      {0, std::nullopt, 49, std::numeric_limits<int32_t>::min(), -1, 0});

  bytedance::bolt::test::assertEqualVectors(
      expected, compareAppVersion(lhs, rhs));
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
