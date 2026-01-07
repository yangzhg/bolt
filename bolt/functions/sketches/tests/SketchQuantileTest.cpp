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

#include <DataSketches/quantiles_sketch.hpp>

#include "SketchTestBase.hpp"
#include "bolt/functions/sketches/SketchQuantiles.cpp"
namespace bytedance::bolt::aggregate::sketches::test {

class SketchQuantileTests : public SketchTestBase {};

TEST_F(SketchQuantileTests, example) {
  using quantiles_float_sketch = datasketches::
      quantiles_sketch<float, std::less<float>, std::allocator<float>>;
  quantiles_float_sketch sketch(datasketches::quantiles_constants::DEFAULT_K);
  const int n = 1'000'000;
  for (int i = 0; i < n; i++) {
    sketch.update(static_cast<float>(i));
    EXPECT_EQ(sketch.get_n(), static_cast<uint64_t>(i + 1));
  }

  EXPECT_EQ(sketch.is_empty(), false);
  EXPECT_EQ(sketch.is_estimation_mode(), true);
  EXPECT_EQ(sketch.get_min_value(), 0.0);
  EXPECT_EQ(sketch.get_quantile(0), 0.0);
  EXPECT_EQ(sketch.get_max_value(), n - 1);
  EXPECT_EQ(sketch.get_quantile(1), n - 1);
  EXPECT_GE(sketch.get_quantile(0.5), n * 0.5 * 0.95);
  EXPECT_GE(n * 0.5 * 1.05, sketch.get_quantile(0.5));

  quantiles_float_sketch sketch2(datasketches::quantiles_constants::DEFAULT_K);
  for (int i = 0; i < n; i++) {
    sketch2.update(static_cast<float>(2 * i));
    EXPECT_EQ(sketch2.get_n(), static_cast<uint64_t>(i + 1));
  }

  EXPECT_EQ(sketch2.is_empty(), false);
  EXPECT_EQ(sketch2.is_estimation_mode(), true);
  EXPECT_EQ(sketch2.get_min_value(), 0.0);
  EXPECT_EQ(sketch2.get_quantile(0), 0.0);
  EXPECT_EQ(sketch2.get_max_value(), 2 * (n - 1));
  EXPECT_EQ(sketch2.get_quantile(1), 2 * (n - 1));
  EXPECT_GE(sketch2.get_quantile(0.5), n * 0.95);
  EXPECT_GE(n * 1.05, sketch2.get_quantile(0.5));

  sketch.merge(sketch2);
  EXPECT_EQ(sketch.is_empty(), false);
  EXPECT_EQ(sketch.is_estimation_mode(), true);
  EXPECT_EQ(sketch.get_min_value(), 0.0);
  EXPECT_EQ(sketch.get_quantile(0), 0.0);
  EXPECT_EQ(sketch.get_max_value(), 2 * (n - 1));
  EXPECT_EQ(sketch.get_quantile(1), 2 * (n - 1));
  auto result = sketch.get_quantile(0.5);
  EXPECT_GE(result, n * 1.5 * 0.8 * 0.5);
  EXPECT_GE(n * 1.5 * 1.2 * 0.5, result);
}

TEST_F(SketchQuantileTests, basic) {
  vector_size_t size = 10'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .partialAggregation({}, {"quantiles(c0, 128, 0.5)"})
                .finalAggregation()
                .planNode();
  int result = readSingleValue(op).value<int>();
  EXPECT_LT(result, 5200);
  EXPECT_LT(4800, result);
}

TEST_F(SketchQuantileTests, singleAggregation) {
  vector_size_t size = 10'000;
  auto values = makeFlatVector<double>(size, [](auto row) { return row; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .singleAggregation({}, {"quantiles(c0, 128, 0.5)"})
                .planNode();

  double result = readSingleValue(op).value<double>();
  EXPECT_LT(result, 6000);
  EXPECT_LT(4000, result);
}

TEST_F(SketchQuantileTests, globalAggFloats) {
  vector_size_t size = 10'000;
  auto values = makeFlatVector<float>(size, [](auto row) { return row * 5; });

  testGlobalAgg(values, "quantiles(c0, 128, 0.5)", 22500.0f, 27500.0f);
}

TEST_F(SketchQuantileTests, globalAggStrings) {
  vector_size_t size = kFruits.size();

  auto values = makeFlatVector<StringView>(
      size, [&](auto row) { return StringView(kFruits[row]); });

  testGlobalAgg(
      values,
      "quantiles(c0, 128, 0.5)",
      kFruits[(size - 3) / 2],
      kFruits[(size + 3) / 2]);
}

TEST_F(SketchQuantileTests, globalAggHighCardinalityIntegers) {
  vector_size_t size = 1'000'000;
  auto values =
      makeFlatVector<int32_t>(size, [size](auto row) { return row + size; });

  testGlobalAgg<int>(
      values, "quantiles(c0, 128, 0.75)", size * 1.73, size * 1.77);
}

TEST_F(SketchQuantileTests, globalAggVeryLowCardinalityIntegers) {
  vector_size_t size = 100;
  auto values =
      makeFlatVector<int32_t>(size, [size](auto row) { return row + size; });

  testGlobalAgg<int>(
      values, "quantiles(c0, 128, 0.75)", size * 1.73, size * 1.77);
}

TEST_F(SketchQuantileTests, globalAggIntegersWithDifferentRank) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  testGlobalAgg(values, "quantiles(c0, 128, 0.0)", 0, 3);
  testGlobalAgg(values, "quantiles(c0, 128, 0.5)", 475, 525);
  testGlobalAgg(values, "quantiles(c0, 128, 1.0)", 950, 1050);
}

TEST_F(SketchQuantileTests, globalAggIntegersWithDifferentK) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  testGlobalAgg(values, "quantiles(c0, 128, 0.0)", 0, 3);
  testGlobalAgg(values, "quantiles(c0, 256, 0.5)", 475, 525);
  testGlobalAgg(values, "quantiles(c0, 64, 1.0)", 950, 1050);
}

TEST_F(SketchQuantileTests, groupByIntegers) {
  vector_size_t size = 10'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? row * 3 : row * 5; });

  std::unordered_map<int32_t, int32_t> expectedResultMin{
      {0, 15000}, {1, 25000}};
  std::unordered_map<int32_t, int32_t> expectedResultMax{
      {0, 15500}, {1, 26000}};

  testGroupByAgg(
      keys,
      values,
      "quantiles(c1, 128, 0.5)",
      expectedResultMin,
      expectedResultMax);
}

TEST_F(SketchQuantileTests, groupByStrings) {
  vector_size_t size = 1'000 * kFruits.size() * kVegetables.size();

  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<StringView>(size, [&](auto row) {
    return StringView(
        row % 2 == 0 ? kFruits[row % kFruits.size()]
                     : kVegetables[row % kVegetables.size()]);
  });

  double fSz = kFruits.size();
  double vSz = kVegetables.size();
  std::unordered_map<int32_t, std::string> expectedMin{
      {0, kFruits[fSz / 2 - 2]}, {1, kVegetables[vSz / 2 - 2]}};
  std::unordered_map<int32_t, std::string> expectedMax{
      {0, kFruits[fSz / 2 + 2]}, {1, kVegetables[vSz / 2 + 2]}};

  testGroupByAgg(
      keys, values, "quantiles(c1, 128, 0.5)", expectedMin, expectedMax);
}

TEST_F(SketchQuantileTests, groupByHighCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  std::unordered_map<int32_t, int32_t> expectedResultMin{{0, 490}, {1, 490}};
  std::unordered_map<int32_t, int32_t> expectedResultMax{{0, 510}, {1, 510}};

  testGroupByAgg(
      keys,
      values,
      "quantiles(c1, 128, 0.5)",
      expectedResultMin,
      expectedResultMax);
}

TEST_F(SketchQuantileTests, groupByVeryLowCardinalityIntegers) {
  vector_size_t size = 1'00;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : row * 2; });

  std::unordered_map<int32_t, int32_t> expectedResultMin{{0, 26}, {1, 90}};
  std::unordered_map<int32_t, int32_t> expectedResultMax{{0, 28}, {1, 110}};

  testGroupByAgg(
      keys,
      values,
      "quantiles(c1, 128, 0.5)",
      expectedResultMin,
      expectedResultMax);
}

TEST_F(SketchQuantileTests, groupByAllNulls) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : row % 3; }, nullEvery(2));

  auto op = PlanBuilder()
                .values({makeRowVector({keys, values})})
                .singleAggregation({"c0"}, {"quantiles(c1, 128, 0.5)"})
                .planNode();

  std::unordered_map<int32_t, int32_t> expectedResult{{0, 0}, {1, 1}};
  auto expectedResultMin = toRowVector(expectedResult);

  expectedResult = {{0, 0}, {1, 3}};
  auto expectedResultMax = toRowVector(expectedResult);

  assertQueryBetween(op, expectedResultMin, expectedResultMax);

  op = PlanBuilder()
           .values({makeRowVector({keys, values})})
           .partialAggregation({"c0"}, {"quantiles(c1, 128, 0.75)"})
           .finalAggregation()
           .planNode();
  assertQueryBetween(op, expectedResultMin, expectedResultMax);
}

TEST_F(SketchQuantileTests, groupByMultiple) {
  vector_size_t size = 1'000;
  auto keys1 = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto keys2 = makeFlatVector<int32_t>(size, [](auto row) { return row % 3; });
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 18; });

  auto plan = PlanBuilder()
                  .values({makeRowVector({keys1, keys2, values})})
                  .partialAggregation({"c0", "c1"}, {"quantiles(c2, 256, 0.5)"})
                  .finalAggregation()
                  .planNode();

  CursorParameters params;
  params.planNode = plan;
  auto result = readCursor(params, [](Task*) {});
  auto actual = result.second;

  auto col3 = actual[0]->childAt(2)->template as<FlatVector<int32_t>>();
  EXPECT_EQ(col3->size(), 2 * 3);
  for (int i = 0; i < col3->size(); i++) {
    EXPECT_BETWEEN(1, col3->valueAt(i), 17);
  }

  result.first->task();
}

TEST_F(SketchQuantileTests, mergeAccumulators) {
  using quantiles_float_sketch = datasketches::
      quantiles_sketch<float, std::less<float>, std::allocator<float>>;

  SketchQuantilesAccumulator<float> accumulator1;
  SketchQuantilesAccumulator<float> accumulator2;

  const int n = 1'000;
  accumulator1.initialize(datasketches::quantiles_constants::DEFAULT_K, 0.5);
  for (int i = 0; i < n; ++i) {
    accumulator1.update(static_cast<float>(i));
  }

  EXPECT_TRUE(accumulator1.hasInitialized());

  accumulator1.merge(accumulator2);

  EXPECT_TRUE(accumulator1.hasInitialized());

  accumulator2.initialize(datasketches::quantiles_constants::DEFAULT_K, 0.5);
  for (int i = 0; i < n; ++i) {
    accumulator2.update(static_cast<float>(i + n));
  }

  accumulator1.merge(accumulator2);

  EXPECT_TRUE(accumulator1.hasInitialized());
}

TEST_F(SketchQuantileTests, mergeInitializedAccumulators) {
  using quantiles_float_sketch = datasketches::
      quantiles_sketch<float, std::less<float>, std::allocator<float>>;

  SketchQuantilesAccumulator<float> accumulator1;
  SketchQuantilesAccumulator<float> accumulator2;

  accumulator1.initialize(datasketches::quantiles_constants::DEFAULT_K, 0.5);
  const int n = 1'000;
  for (int i = 0; i < n; ++i) {
    accumulator1.update(static_cast<float>(i));
  }

  accumulator2.initialize(datasketches::quantiles_constants::DEFAULT_K, 0.5);
  for (int i = 0; i < n; ++i) {
    accumulator2.update(static_cast<float>(i + n));
  }

  EXPECT_TRUE(accumulator1.hasInitialized());
  EXPECT_TRUE(accumulator2.hasInitialized());

  accumulator1.merge(accumulator2);

  EXPECT_TRUE(accumulator1.hasInitialized());
}

TEST_F(SketchQuantileTests, mergeWithUninitializedAccumulator) {
  using quantiles_float_sketch = datasketches::
      quantiles_sketch<float, std::less<float>, std::allocator<float>>;

  SketchQuantilesAccumulator<float> accumulator1;
  SketchQuantilesAccumulator<float> accumulator2;

  accumulator1.initialize(datasketches::quantiles_constants::DEFAULT_K, 0.5);
  const int n = 1'000;
  for (int i = 0; i < n; ++i) {
    accumulator1.update(static_cast<float>(i));
  }

  EXPECT_TRUE(accumulator1.hasInitialized());
  EXPECT_FALSE(accumulator2.hasInitialized());

  accumulator2.merge(accumulator1);

  EXPECT_TRUE(accumulator2.hasInitialized());
}

} // namespace bytedance::bolt::aggregate::sketches::test
