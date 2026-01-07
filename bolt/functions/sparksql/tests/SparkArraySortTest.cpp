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

#include <optional>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/sparksql/tests/ArraySortTestData.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "bolt/vector/ComplexVector.h"
namespace bytedance::bolt::functions::sparksql::test {
namespace {
using namespace bytedance::bolt::test;

using bytedance::bolt::functions::test::FunctionBaseTest;

class SparkArraySortTest : public SparkFunctionBaseTest {
 protected:
  void testArraySort(const VectorPtr& input, const VectorPtr& expected) {
    auto result = evaluate("array_sort(c0)", makeRowVector({input}));
    assertEqualVectors(expected, result);
  }

  void testArraySort(
      const std::string& lamdaExpr,
      bool asc,
      const VectorPtr& input,
      const VectorPtr& expected) {
    std::string name = asc ? "array_sort" : "array_sort_desc";
    auto result = evaluate(
        fmt::format("{}(c0, {})", name, lamdaExpr), makeRowVector({input}));
    assertEqualVectors(expected, result);

    SelectivityVector firstRow(1);
    result = evaluate(
        fmt::format("{}(c0, {})", name, lamdaExpr),
        makeRowVector({input}),
        firstRow);
    assertEqualVectors(expected->slice(0, 1), result);
  }

  template <typename T>
  void testInt() {
    auto input = makeNullableArrayVector(intInput<T>());
    auto expected = makeNullableArrayVector(intAscNullLargest<T>());
    testArraySort(input, expected);
  }

  template <typename T>
  void testFloatingPoint() {
    auto input = makeNullableArrayVector(floatingPointInput<T>());
    auto expected = makeNullableArrayVector(floatingPointAscNullLargest<T>());
    testArraySort(input, expected);
  }
};

TEST_F(SparkArraySortTest, int8) {
  testInt<int8_t>();
}

TEST_F(SparkArraySortTest, int16) {
  testInt<int16_t>();
}

TEST_F(SparkArraySortTest, int32) {
  testInt<int32_t>();
}

TEST_F(SparkArraySortTest, int64) {
  testInt<int64_t>();
}

TEST_F(SparkArraySortTest, float) {
  testFloatingPoint<float>();
}

TEST_F(SparkArraySortTest, double) {
  testFloatingPoint<double>();
}

TEST_F(SparkArraySortTest, string) {
  auto input = makeNullableArrayVector(stringInput());
  auto expected = makeNullableArrayVector(stringAscNullLargest());
  testArraySort(input, expected);
}

TEST_F(SparkArraySortTest, timestamp) {
  auto input = makeNullableArrayVector(timestampInput());
  auto expected = makeNullableArrayVector(timestampAscNullLargest());
  testArraySort(input, expected);
}

TEST_F(SparkArraySortTest, date) {
  auto input = makeNullableArrayVector(dateInput(), ARRAY(DATE()));
  auto expected = makeNullableArrayVector(dateAscNullLargest(), ARRAY(DATE()));
  testArraySort(input, expected);
}

TEST_F(SparkArraySortTest, bool) {
  auto input = makeNullableArrayVector(boolInput());
  auto expected = makeNullableArrayVector(boolAscNullLargest());
  testArraySort(input, expected);
}

TEST_F(SparkArraySortTest, array) {
  auto input = makeNullableNestedArrayVector(arrayInput());
  auto expected = makeNullableNestedArrayVector(arrayAscNullLargest());
  testArraySort(input, expected);
}

// Map is not orderable, so sorting is not supported.
TEST_F(SparkArraySortTest, failOnMapTypeSort) {
  auto input = makeArrayOfMapVector(mapInput());
  const std::string kErrorMessage =
      "Scalar function signature is not supported"_sv;

  BOLT_ASSERT_THROW(
      evaluate("array_sort(c0)", makeRowVector({input})), kErrorMessage);
}

TEST_F(SparkArraySortTest, row) {
  auto rowType = ROW({INTEGER(), VARCHAR()});
  auto input = makeArrayOfRowVector(rowType, rowInput());
  auto expected = makeArrayOfRowVector(rowType, rowAscNullLargest());
  testArraySort(input, expected);
}

TEST_F(SparkArraySortTest, constant) {
  vector_size_t size = 1'000;
  auto data =
      makeArrayVector<int64_t>({{1, 2, 3, 0}, {4, 5, 4, 5}, {6, 6, 6, 6}});

  auto evaluateConstant = [&](vector_size_t row, const VectorPtr& vector) {
    return evaluate(
        "array_sort(c0)",
        makeRowVector({BaseVector::wrapInConstant(size, row, vector)}));
  };

  auto result = evaluateConstant(0, data);
  auto expected = makeConstantArray<int64_t>(size, {0, 1, 2, 3});
  assertEqualVectors(expected, result);

  result = evaluateConstant(1, data);
  expected = makeConstantArray<int64_t>(size, {4, 4, 5, 5});
  assertEqualVectors(expected, result);

  result = evaluateConstant(2, data);
  expected = makeConstantArray<int64_t>(size, {6, 6, 6, 6});
  assertEqualVectors(expected, result);
}

TEST_F(SparkArraySortTest, lambda) {
  auto data = makeNullableArrayVector<std::string>({
      {"abc123", "abc", std::nullopt, "abcd"},
      {std::nullopt, "x", "xyz123", "xyz"},
  });

  auto sortedAsc = makeNullableArrayVector<std::string>({
      {"abc", "abcd", "abc123", std::nullopt},
      {"x", "xyz", "xyz123", std::nullopt},
  });

  auto sortedDesc = makeNullableArrayVector<std::string>({
      {"abc123", "abcd", "abc", std::nullopt},
      {"xyz123", "xyz", "x", std::nullopt},
  });

  // Different ways to sort by length ascending.
  testArraySort("x -> length(x)", true, data, sortedAsc);
  testArraySort("x -> length(x) * -1", false, data, sortedAsc);
  testArraySort(
      "(x, y) -> if(lessthan(length(x), length(y)), -1, if(greaterthan(length(x), length(y)), 1, 0))",
      true,
      data,
      sortedAsc);
  testArraySort(
      "(x, y) -> if(lessthan(length(x), length(y)), -1, if(equalto(length(x), length(y)), 0, 1))",
      true,
      data,
      sortedAsc);

  // Different ways to sort by length descending.
  testArraySort("x -> length(x)", false, data, sortedDesc);
  testArraySort("x -> length(x) * -1", true, data, sortedDesc);
  testArraySort(
      "(x, y) -> if(lessthan(length(x), length(y)), 1, if(greaterthan(length(x), length(y)), -1, 0))",
      true,
      data,
      sortedDesc);
  testArraySort(
      "(x, y) -> if(lessthan(length(x), length(y)), 1, if(equalto(length(x), length(y)), 0, -1))",
      true,
      data,
      sortedDesc);

  // Lambda function return NULL.
  BOLT_ASSERT_THROW(
      evaluate(
          "array_sort(c0, (x, y) -> IF(lessthan(x, y), 1, IF(equalto(x, y), 0, null)))",
          makeRowVector({data})),
      "Else clause of a SWITCH statement must have the same type as 'then' clauses. Expected BIGINT, but got UNKNOWN.");
}

TEST_F(SparkArraySortTest, unsupporteLambda) {
  auto data = makeRowVector({
      makeNullableArrayVector(intInput<int32_t>()),
  });

  BOLT_ASSERT_THROW(
      evaluate("array_sort(c0, (a, b) -> 0)", data),
      "array_sort with comparator lambda that cannot be rewritten into a transform is not supported");
}
} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
