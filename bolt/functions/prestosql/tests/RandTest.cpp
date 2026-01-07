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
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/prestosql/tests/utils/FunctionBaseTest.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
namespace bytedance::bolt::functions {

namespace {

class RandTest : public functions::test::FunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> random(T n) {
    return evaluateOnce<T>("random(c0)", std::make_optional(n));
  }

  template <typename T>
  std::optional<T> rand(T n) {
    return evaluateOnce<T>("rand(c0)", std::make_optional(n));
  }

  template <typename T>
  std::optional<T> randomWithTry(T n) {
    return evaluateOnce<T>("try(random(c0))", std::make_optional(n));
  }

  template <typename T>
  std::optional<T> randWithTry(T n) {
    return evaluateOnce<T>("try(rand(c0))", std::make_optional(n));
  }
};

TEST_F(RandTest, zeroArg) {
  auto result = evaluateOnce<double>("random()", makeRowVector(ROW({}), 1));
  EXPECT_LT(result, 1.0);
  EXPECT_GE(result, 0.0);

  result = evaluateOnce<double>("rand()", makeRowVector(ROW({}), 1));
  EXPECT_LT(result, 1.0);
  EXPECT_GE(result, 0.0);
}

TEST_F(RandTest, negativeInt32) {
  BOLT_ASSERT_THROW(random(-5), "bound must be positive");
  ASSERT_EQ(randomWithTry(-5), std::nullopt);

  BOLT_ASSERT_THROW(rand(-5), "bound must be positive");
  ASSERT_EQ(randWithTry(-5), std::nullopt);
}

TEST_F(RandTest, nonNullInt64) {
  int64_t data = 346;
  EXPECT_LT(random(data), data);
  EXPECT_LT(rand(data), data);
}

TEST_F(RandTest, nonNullInt16) {
  int16_t data = 346;
  EXPECT_LT(random(data), data);
  EXPECT_LT(rand(data), data);
}

TEST_F(RandTest, nonNullInt8) {
  int8_t data = 4;
  EXPECT_LT(random(data), data);
  EXPECT_LT(rand(data), data);
}

} // namespace
} // namespace bytedance::bolt::functions
