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
#include "bolt/functions/flinksql/tests/FlinkFunctionBaseTest.h"
namespace bytedance::bolt::functions::flinksql::test {

namespace {
class RandTest : public FlinkFunctionBaseTest {
 public:
  RandTest() {
    // Allow for parsing literal integers as INTEGER, not BIGINT.
    options_.parseIntegerAsBigint = false;
  }
};

TEST_F(RandTest, rand_integer) {
  auto check_result = [](std::optional<int32_t> max,
                         std::optional<int32_t> result) {
    if (max == std::nullopt) {
      EXPECT_EQ(result, std::nullopt);
    } else if (max.value() == 0) {
      EXPECT_NE(result, std::nullopt);
      EXPECT_EQ(result.value(), 0);
    } else {
      EXPECT_NE(result, std::nullopt);
      EXPECT_GE(result.value(), 0);
      EXPECT_LT(result.value(), max);
    }
  };

  auto rand_integer = [this, &check_result](std::optional<int32_t> max) {
    return evaluateOnce<int32_t>(
        fmt::format("rand_integer(c0)"),
        makeRowVector({makeNullableFlatVector<int32_t>({max})}));
  };

  auto rand_integer_seed = [this, &check_result](
                               std::optional<int32_t> max, int32_t seed) {
    return evaluateOnce<int32_t>(
        fmt::format("rand_integer(c0, {})", seed),
        makeRowVector({makeNullableFlatVector<int32_t>({max})}));
  };

  {
    auto result = rand_integer(0);
    check_result(0, result);
  }
  {
    auto result = rand_integer(10000000);
    check_result(10000000, result);
  }
  {
    auto result1 = rand_integer(10);
    check_result(10, result1);
  }
  {
    auto result = rand_integer_seed(0, 100);
    check_result(0, result);
  }
  {
    auto result = rand_integer_seed(10000000, 10);
    check_result(10000000, result);
  }
  {
    auto result1 = rand_integer_seed(100, 5);
    check_result(100, result1);
    auto result2 = rand_integer_seed(100, 5);
    check_result(100, result2);
    EXPECT_EQ(result1.value(), result2.value());
  }
}
} // namespace
} // namespace bytedance::bolt::functions::flinksql::test