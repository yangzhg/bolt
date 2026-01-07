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

#include <DataSketches/var_opt_sketch.hpp>
#include <DataSketches/var_opt_union.hpp>

#include "SketchTestBase.hpp"
namespace bytedance::bolt::aggregate::sketches::test {

class SketchVarOptTests : public SketchTestBase {};

TEST_F(SketchVarOptTests, example) {
  double weight = 1.0;
  int n = 1'000'000;
  datasketches::var_opt_sketch<int> sketch1(64);
  for (uint64_t i = 0; i < n; ++i) {
    sketch1.update(static_cast<int>(i), weight);
  }
  std::vector<uint8_t> bytes1 = sketch1.serialize();

  datasketches::var_opt_sketch<int> sketch2(64);
  for (uint64_t i = n; i < 2 * n - 1; ++i) {
    sketch1.update(static_cast<int>(i), weight);
  }
  std::vector<uint8_t> bytes2 = sketch1.serialize();

  datasketches::var_opt_sketch<int> sketch3 =
      datasketches::var_opt_sketch<int>::deserialize(
          bytes1.data(), bytes1.size());
  datasketches::var_opt_sketch<int> sketch4 =
      datasketches::var_opt_sketch<int>::deserialize(
          bytes2.data(), bytes2.size());

  datasketches::var_opt_union<int> u(64);
  u.update(sketch3);
  u.update(sketch4);

  datasketches::var_opt_sketch<int> result_sketch = u.get_result();
  datasketches::subset_summary ss =
      result_sketch.estimate_subset_sum([](int x) { return x >= 100; });
  EXPECT_LE(0, ss.estimate);
  EXPECT_LE((100 + 64 / 2) * 64, ss.total_sketch_weight);
  EXPECT_LE((100 + 64 / 2) * 64, ss.lower_bound);
  EXPECT_GE(ss.upper_bound, n * (2 * n - 1) / 2);
}

TEST_F(SketchVarOptTests, basic) {
  vector_size_t size = 10'000;
  auto values = makeFlatVector<int64_t>(size, [](auto row) { return row; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .partialAggregation({}, {"var_opt(c0, 200, 0)"})
                .finalAggregation()
                .planNode();
  double result = (int)readSingleValue(op).value<double>();
  EXPECT_LE(result, 10100);
  EXPECT_LE(0, result);
}

TEST_F(SketchVarOptTests, singleAggregation) {
  vector_size_t size = 10'000;
  auto values = makeFlatVector<double>(size, [](auto row) { return row; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .singleAggregation({}, {"var_opt(c0, 1024, 10.0)"})
                .planNode();

  double result = readSingleValue(op).value<double>();
  EXPECT_LT(result, 10050);
  EXPECT_LT(9950, result);
}

TEST_F(SketchVarOptTests, globalAggFloats) {
  vector_size_t size = 10'000;
  auto values = makeFlatVector<double>(size, [](auto row) { return row * 5; });

  testGlobalAgg<double>(values, "var_opt(c0, 1024,500.0)", 9500.0, 10500.0);
}

TEST_F(SketchVarOptTests, globalAggHighCardinalityIntegers) {
  vector_size_t size = 10'000;
  auto values =
      makeFlatVector<int32_t>(size, [size](auto row) { return row + size; });

  testGlobalAgg<double>(values, "var_opt(c0, 1024, 1000)", size / 2, size * 2);
}

TEST_F(SketchVarOptTests, globalAggVeryLowCardinalityIntegers) {
  vector_size_t size = 100;
  auto values =
      makeFlatVector<int32_t>(size, [size](auto row) { return row + size; });

  testGlobalAgg<double>(values, "var_opt(c0, 1024, 100)", size / 2, size * 2);
}

TEST_F(SketchVarOptTests, globalAggIntegersWithDifferentRank) {
  vector_size_t size = 1'000'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  testGlobalAgg<double>(values, "var_opt(c0, 1024, 0)", size / 2, size * 2);
  testGlobalAgg<double>(values, "var_opt(c0, 1024, 10)", size / 2, size * 2);
  testGlobalAgg<double>(values, "var_opt(c0, 1024, 100)", size / 2, size * 2);
}

TEST_F(SketchVarOptTests, groupByIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<double>(
      size, [](auto row) { return row % 2 == 0 ? row % 17 : row % 21 + 100; });

  std::unordered_map<int32_t, double> expectedResultMin{{0, 0}, {1, 450.0}};
  std::unordered_map<int32_t, double> expectedResultMax{{0, 20}, {1, 550.0}};

  testGroupByAgg(
      keys,
      values,
      "var_opt(c1, 1024, 100.0)",
      expectedResultMin,
      expectedResultMax);
}

TEST_F(SketchVarOptTests, groupByHighCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<double>(size, [](auto row) { return row; });

  std::unordered_map<int32_t, double> expectedResultMin{{0, 490.0}, {1, 490.0}};
  std::unordered_map<int32_t, double> expectedResultMax{{0, 510.0}, {1, 500.0}};

  testGroupByAgg<int32_t, double>(
      keys,
      values,
      "var_opt(c1, 1024, 5.0)",
      expectedResultMin,
      expectedResultMax);
}

TEST_F(SketchVarOptTests, groupByVeryLowCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<double>(
      size, [](auto row) { return row % 2 == 0 ? 27 : row % 3; });

  std::unordered_map<int32_t, double> expectedResultMin{{0, 200}, {1, 200}};
  std::unordered_map<int32_t, double> expectedResultMax{{0, 700}, {1, 400}};

  testGroupByAgg(
      keys,
      values,
      "var_opt(c1, 1024, 1.0)",
      expectedResultMin,
      expectedResultMax);
}

TEST_F(SketchVarOptTests, groupByAllNulls) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<double>(
      size, [](auto row) { return row % 2 == 0 ? 27 : 35; }, nullEvery(2));

  std::unordered_map<int32_t, double> expectedResultMin = {{0, 0}, {1, 495}};
  std::unordered_map<int32_t, double> expectedResultMax = {{0, 0}, {1, 505}};
  auto expectedMin = toRowVector(expectedResultMin);
  auto expectedMax = toRowVector(expectedResultMax);
  auto op = PlanBuilder()
                .values({makeRowVector({keys, values})})
                .singleAggregation({"c0"}, {"var_opt(c1, 1024, 1.0)"})
                .planNode();
  assertQueryBetween(op, expectedMin, expectedMax);

  op = PlanBuilder()
           .values({makeRowVector({keys, values})})
           .partialAggregation({"c0"}, {"var_opt(c1, 1024, 1.0)"})
           .finalAggregation()
           .planNode();
  assertQueryBetween(op, expectedMin, expectedMax);
}

TEST_F(SketchVarOptTests, groupByMultiple) {
  vector_size_t size = 1'000;
  auto keys1 = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto keys2 = makeFlatVector<int32_t>(size, [](auto row) { return row % 3; });
  auto values = makeFlatVector<double>(size, [](auto row) { return row % 18; });

  auto plan = PlanBuilder()
                  .values({makeRowVector({keys1, keys2, values})})
                  .partialAggregation({"c0", "c1"}, {"var_opt(c2, 1024, 1.0)"})
                  .finalAggregation()
                  .planNode();

  CursorParameters params;
  params.planNode = plan;
  auto result = readCursor(params, [](Task*) {});
  auto actual = result.second;

  auto col3 = actual[0]->childAt(2)->template as<FlatVector<double>>();
  EXPECT_EQ(col3->size(), 2 * 3);
  for (int i = 0; i < col3->size(); i++) {
    EXPECT_BETWEEN(1, col3->valueAt(i), 250);
  }

  result.first->task();
}

} // namespace bytedance::bolt::aggregate::sketches::test
