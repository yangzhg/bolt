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

#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/lib/aggregates/tests/AggregationTestBase.h"
#include "bolt/functions/sparksql/aggregates/Register.h"
namespace bytedance::bolt::functions::sparksql::aggregates::test {

namespace {

class CollectSetAggregateTest : public aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    aggregate::test::AggregationTestBase::SetUp();
    aggregate::sparksql::registerAggregateFunctions("");
  }

 public:
  template <typename T>
  void testGroupBy() {
    auto vectors = {makeRowVector({
        makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
        makeFlatVector<T>(
            98, // size
            [](auto row) { return row % 2; }, // valueAt
            [](auto row) { return row % 3 == 0; }), // nullAt
    })};

    createDuckDbTable(vectors);

    // Verify when ignoreNull is true.
    testAggregations(
        vectors,
        {"c0"},
        {"collect_set(c1)"},
        "SELECT c0, array_agg(distinct c1) FROM tmp where c1 is not null GROUP BY c0");

    /*auto expected = {makeRowVector({
        makeFlatVector<int32_t>(7, [](auto row) { return row; }),
        makeFlatVector<T>(
            7, // size
            [](auto row) { return 91 + row; }, // valueAt
            [](auto row) { return (91 + row) % 3 == 0; }), // nullAt
    })};
    testAggregations(vectors, {"c0"}, {"last(c1, c3)"}, expected);

    // Verify when ignoreNull is not provided. Defaults to false.
    testAggregations(vectors, {"c0"}, {"last(c1)"}, expected);*/
  }
};

// Verify aggregation with group by keys for TINYINT.
TEST_F(CollectSetAggregateTest, tinyIntGroupBy) {
  testGroupBy<int8_t>();
}

// Verify aggregation with group by keys for SMALLINT.
TEST_F(CollectSetAggregateTest, smallIntGroupBy) {
  testGroupBy<int16_t>();
}

// Verify aggregation with group by keys for INTEGER.
TEST_F(CollectSetAggregateTest, integerGroupBy) {
  testGroupBy<int32_t>();
}

// Verify aggregation with group by keys for BIGINT.
TEST_F(CollectSetAggregateTest, bigintGroupBy) {
  testGroupBy<int64_t>();
}

// Verify aggregation with group by keys for REAL.
TEST_F(CollectSetAggregateTest, realGroupBy) {
  testGroupBy<float>();
}

// Verify aggregation with group by keys for DOUBLE.
TEST_F(CollectSetAggregateTest, doubleGroupBy) {
  testGroupBy<double>();
}

// Vefify aggregation with group by keys for VARCHAR.
TEST_F(CollectSetAggregateTest, varcharGroupBy) {
  std::vector<std::string> data(98);
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeFlatVector<StringView>(
          98, // size
          [&data](auto row) {
            data[row] = std::to_string(row % 2);
            return StringView(data[row]);
          }, // valueAt
          [](auto row) { return row % 3 == 0; }), // nullAt
  })};

  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {"c0"},
      {"collect_set(c1)"},
      "SELECT c0, array_agg(distinct c1) FROM tmp WHERE c1 IS NOT NULL GROUP BY c0");

  std::vector<std::string> data1(98);
  auto vectors1 = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeFlatVector<StringView>(
          98, // size
          [&data1](auto row) {
            data1[row] = std::string("larger than prefix Size 12");
            return StringView(data1[row]);
          }, // valueAt
          [](auto row) { return row % 3 == 0; }), // nullAt
  })};

  createDuckDbTable(vectors1);

  testAggregations(
      vectors1,
      {"c0"},
      {"collect_set(c1)"},
      "SELECT c0, array_agg(distinct c1) FROM tmp WHERE c1 IS NOT NULL GROUP BY c0");
}

// Verify aggregation with group by keys for ARRAY.
TEST_F(CollectSetAggregateTest, bigintArrayGroupBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row % 2; }),
      makeNullableArrayVector<int64_t>(
          {{1, 2, 3},
           {33, 44},
           {1, 2, 3},
           {100, 200, 300},
           {3, 1, 2},
           {100, std::nullopt},
           {std::nullopt}}),
  })};

  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {"c0"},
      {"collect_set(c1)"},
      "SELECT c0, array_agg(distinct c1) FROM tmp WHERE c1 IS NOT NULL GROUP BY c0");
}

TEST_F(CollectSetAggregateTest, varcharArrayGroupBy) {
  using S = StringView;
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row % 2; }),
      makeNullableArrayVector<StringView>({
          {S("red shiny car ahead"), S("blue clear sky above")},
          {S("blue clear sky above"), S("red shiny car ahead")},
          {S("blue clear sky above"), S("red shiny car ahead")},
          {S("blue clear sky above"), S("red shiny car ahead")},
          {S("a"), S("b"), std::nullopt},
          {S("a"), S("b"), std::nullopt},
          {S("a"), S("b"), std::nullopt},
      }),
  })};

  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {"c0"},
      {"collect_set(c1)"},
      "SELECT c0, array_agg(distinct c1) FROM tmp WHERE c1 IS NOT NULL GROUP BY c0");
}

/*
// Verify global aggregation for ARRAY.
TEST_F(CollectSetAggregateTest, arrayGlobal) {
  auto vectors = {makeRowVector({
      makeNullableArrayVector<int64_t>(
          {std::nullopt, {{1, 2}}, {{3, 4}}, std::nullopt}),
      makeConstant<bool>(true, 4),
      makeConstant<bool>(false, 4),
  })};

  auto expectedTrue = {makeRowVector({
      makeArrayVector<int64_t>({{3, 4}}),
  })};

  // Verify when ignoreNull is true.
  testAggregations(vectors, {}, {"last(c0, c1)"}, expectedTrue);

  // Verify when ignoreNull is false.
  auto expectedFalse = {makeRowVector({
      makeNullableArrayVector<int64_t>({std::nullopt}),
  })};
  testAggregations(vectors, {}, {"last(c0, c2)"}, expectedFalse);

  // Verify when ignoreNull is not provided. Defaults to false.
  testAggregations(vectors, {}, {"last(c0)"}, expectedFalse);
}

// Verify aggregation with group by keys for MAP column.
TEST_F(CollectSetAggregateTest, mapGroupBy) {
  auto vectors = {makeRowVector({
      makeFlatVector<int32_t>(98, [](auto row) { return row % 7; }),
      makeMapVector<int64_t, float>(
          98, // size
          [](auto row) { return row % 2 ? 0 : 2; }, // sizeAt
          [](auto idx) { return idx; }, // keyAt
          [](auto idx) { return idx * 0.1; }), // valueAt
  })};

  // Expected result should have last 7 rows of input |vectors|
  auto expected = {makeRowVector({
      makeFlatVector<int32_t>(7, [](auto row) { return row; }),
      makeMapVector<int64_t, float>(
          7, // size
          [](auto row) { return row % 2 ? 2 : 0; }, // sizeAt
          [](auto idx) { return 92 + idx; }, // keyAt
          [](auto idx) { return (92 + idx) * 0.1; }), // valueAt
  })};

  testAggregations(vectors, {"c0"}, {"last(c1)"}, expected);
}

// Verify global aggregation for MAP column.
TEST_F(CollectSetAggregateTest, mapGlobal) {
  auto O = [](const std::vector<std::pair<int64_t, std::optional<float>>>& m) {
    return std::make_optional(m);
  };
  auto vectors = {makeRowVector({
      makeNullableMapVector<int64_t, float>(
          {std::nullopt, O({{1, 2.0}}), O({{2, 4.0}}), std::nullopt}),
      makeConstant<bool>(true, 4),
      makeConstant<bool>(false, 4),
  })};

  auto expectedTrue = {makeRowVector({
      makeNullableMapVector<int64_t, float>({O({{2, 4.0}})}),
  })};

  // Verify when ignoreNull is true.
  testAggregations(vectors, {}, {"last(c0, c1)"}, expectedTrue);

  // Verify when ignoreNull is false.
  auto expectedFalse = {makeRowVector({
      makeNullableMapVector<int64_t, float>({std::nullopt}),
  })};
  testAggregations(vectors, {}, {"last(c0, c2)"}, expectedFalse);

  // Verify when ignoreFalse is not provided. Defaults to false.
  testAggregations(vectors, {}, {"last(c0)"}, expectedFalse);
}
*/
} // namespace
} // namespace bytedance::bolt::functions::sparksql::aggregates::test
