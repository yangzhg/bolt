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

#include <vector/tests/utils/VectorTestBase.h>
#include <optional>
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::functions::test;
namespace bytedance::bolt::functions::sparksql::test {
namespace {
class DecimalVectorFunctionTest : public SparkFunctionBaseTest {
 protected:
  template <TypeKind KIND>
  void testDecimalExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    using EvalType = typename bolt::TypeTraits<KIND>::NativeType;
    auto result =
        evaluate<SimpleVector<EvalType>>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
    testOpDictVectors<EvalType>(expression, expected, input);
  }

  template <typename T>
  void testOpDictVectors(
      const std::string& operation,
      const VectorPtr& expected,
      const std::vector<VectorPtr>& flatVector) {
    // Dictionary vectors as arguments.
    auto newSize = flatVector[0]->size() * 2;
    std::vector<VectorPtr> dictVectors;
    for (auto i = 0; i < flatVector.size(); ++i) {
      auto indices = makeIndices(newSize, [&](int row) { return row / 2; });
      dictVectors.push_back(
          VectorTestBase::wrapInDictionary(indices, newSize, flatVector[i]));
    }
    auto resultIndices = makeIndices(newSize, [&](int row) { return row / 2; });
    auto expectedResultDictionary =
        VectorTestBase::wrapInDictionary(resultIndices, newSize, expected);
    auto actual =
        evaluate<SimpleVector<T>>(operation, makeRowVector(dictVectors));
    assertEqualVectors(expectedResultDictionary, actual);
  }
};

TEST_F(DecimalVectorFunctionTest, DISABLED_makeDecimal) {
  testDecimalExpr<TypeKind::BIGINT>(
      {makeFlatVector<int64_t>({1111, -1112, 9999, 0}, DECIMAL(5, 1))},
      "make_decimal_by_unscaled_value(c0, c1, true)",
      {makeFlatVector<int64_t>({1111, -1112, 9999, 0}),
       makeConstant<int64_t>(0, 4, DECIMAL(5, 1))});
  testDecimalExpr<TypeKind::HUGEINT>(
      {makeFlatVector<int128_t>(
          {11111111, -11112112, 99999999, DecimalUtil::kShortDecimalMax + 1},
          DECIMAL(38, 19))},
      "make_decimal_by_unscaled_value(c0, c1, true)",
      {makeFlatVector<int64_t>(
           {11111111, -11112112, 99999999, DecimalUtil::kShortDecimalMax + 1}),
       makeConstant<int128_t>(0, 4, DECIMAL(38, 19))});

  testDecimalExpr<TypeKind::BIGINT>(
      {makeNullableFlatVector<int64_t>(
          {101, std::nullopt, std::nullopt}, DECIMAL(3, 1))},
      "make_decimal_by_unscaled_value(c0, c1, true)",
      {makeNullableFlatVector<int64_t>({101, std::nullopt, 1000}),
       makeConstant<int64_t>(0, 3, DECIMAL(3, 1))});
}

TEST_F(DecimalVectorFunctionTest, DISABLED_unscale) {
  auto expected =
      makeNullableFlatVector<int64_t>({1111111, std::nullopt, 10000});
  auto input = makeNullableFlatVector<int64_t>(
      {1111111, std::nullopt, 10000}, DECIMAL(15, 10));
  auto result = evaluate<FlatVector<int64_t>>(
      "unscaled_value(c0)", makeRowVector({input}));
  assertEqualVectors(result, expected);
}

TEST_F(DecimalVectorFunctionTest, compare) {
  auto dec1 = makeNullableFlatVector<int64_t>(
      {124000000L, 341234567L, 391234578L, std::nullopt, 345L}, DECIMAL(18, 7));
  auto dec2 = makeNullableFlatVector<int64_t>(
      {1240000, 12845678L, 1298765L, 123L, std::nullopt}, DECIMAL(18, 5));
  {
    auto expectedResult = makeNullableFlatVector<bool>(
        {true, false, false, std::nullopt, std::nullopt});
    auto result = evaluate<SimpleVector<bool>>(
        "decimal_eq(c0, c1)", makeRowVector({dec1, dec2}));
    assertEqualVectors(expectedResult, result);
  }

  {
    auto expectedResult = makeNullableFlatVector<bool>(
        {false, true, false, std::nullopt, std::nullopt});
    auto result = evaluate<SimpleVector<bool>>(
        "decimal_lt(c0, c1)", makeRowVector({dec1, dec2}));
    assertEqualVectors(expectedResult, result);
  }

  {
    auto expectedResult = makeNullableFlatVector<bool>(
        {true, true, false, std::nullopt, std::nullopt});
    auto result = evaluate<SimpleVector<bool>>(
        "decimal_lte(c0, c1)", makeRowVector({dec1, dec2}));
    assertEqualVectors(expectedResult, result);
  }

  {
    auto expectedResult = makeNullableFlatVector<bool>(
        {false, false, true, std::nullopt, std::nullopt});
    auto result = evaluate<SimpleVector<bool>>(
        "decimal_gt(c0, c1)", makeRowVector({dec1, dec2}));
    assertEqualVectors(expectedResult, result);
  }

  {
    auto expectedResult = makeNullableFlatVector<bool>(
        {true, false, true, std::nullopt, std::nullopt});
    auto result = evaluate<SimpleVector<bool>>(
        "decimal_gte(c0, c1)", makeRowVector({dec1, dec2}));
    assertEqualVectors(expectedResult, result);
  }
}

TEST_F(DecimalVectorFunctionTest, round) {
  auto input =
      makeFlatVector<int64_t>({111111, 123456, 123478}, DECIMAL(15, 2));

  {
    auto expected = makeFlatVector<int64_t>({1112, 1235, 1235}, DECIMAL(14, 0));
    auto result =
        evaluate<SimpleVector<int64_t>>("ceil(c0)", makeRowVector({input}));
    assertEqualVectors(result, expected);
  }

  {
    auto expected = makeFlatVector<int64_t>({1111, 1234, 1234}, DECIMAL(14, 0));
    auto result =
        evaluate<SimpleVector<int64_t>>("floor(c0)", makeRowVector({input}));
    assertEqualVectors(result, expected);
  }
}
} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
