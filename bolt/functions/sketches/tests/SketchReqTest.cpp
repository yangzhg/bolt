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

#include <DataSketches/req_sketch.hpp>

#include "SketchTestBase.hpp"
namespace bytedance::bolt::aggregate::sketches::test {

class SketchREQTests : public SketchTestBase {};

TEST_F(SketchREQTests, example) {
  datasketches::req_sketch<float> sketch1(12);
  for (size_t i = 0; i < 40; ++i)
    sketch1.update(static_cast<float>(i));

  datasketches::req_sketch<float> sketch2(12);
  for (size_t i = 40; i < 80; ++i)
    sketch2.update(static_cast<float>(i));

  datasketches::req_sketch<float> sketch3(12);
  for (size_t i = 80; i < 120; ++i)
    sketch3.update(static_cast<float>(i));

  datasketches::req_sketch<float> sketch(12);
  sketch.merge(sketch1);
  sketch.merge(sketch2);
  sketch.merge(sketch3);
  EXPECT_EQ(sketch.get_min_value(), 0);
  EXPECT_EQ(sketch.get_max_value(), 119);
  EXPECT_GE(sketch.get_quantile(0.5), 60 - 3);
  EXPECT_LE(sketch.get_quantile(0.5), 60 + 3);
  EXPECT_GE(sketch.get_rank(60.0f), 0.5 - 0.01);
  EXPECT_LE(sketch.get_rank(60.0f), 0.5 + 0.01);
}

TEST_F(SketchREQTests, basic) {
  vector_size_t size = 10'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .partialAggregation({}, {"req(c0, 12, 0.5)"})
                .finalAggregation()
                .planNode();
  int result = readSingleValue(op).value<int>();
  EXPECT_LT(result, 5100);
  EXPECT_LT(4900, result);
}

TEST_F(SketchREQTests, singleAggregation) {
  vector_size_t size = 10'000;
  auto values = makeFlatVector<double>(size, [](auto row) { return row; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .singleAggregation({}, {"req(c0, 12, 0.5)"})
                .planNode();

  double result = readSingleValue(op).value<double>();
  EXPECT_LT(result, 6000);
  EXPECT_LT(4000, result);
}

TEST_F(SketchREQTests, globalAggFloats) {
  vector_size_t size = 10'000;
  auto values = makeFlatVector<float>(size, [](auto row) { return row * 5; });

  testGlobalAgg(values, "req(c0, 12, 0.5)", 24500.0f, 25500.0f);
}

TEST_F(SketchREQTests, globalAggStrings) {
  vector_size_t size = kFruits.size();

  auto values = makeFlatVector<StringView>(
      size, [&](auto row) { return StringView(kFruits[row]); });

  testGlobalAgg(
      values,
      "req(c0, 12, 0.5)",
      kFruits[(size - 3) / 2],
      kFruits[(size + 3) / 2]);
}

TEST_F(SketchREQTests, globalAggHighCardinalityIntegers) {
  vector_size_t size = 1'000'000;
  auto values =
      makeFlatVector<int32_t>(size, [size](auto row) { return row + size; });

  testGlobalAgg<int>(values, "req(c0, 12, 0.75)", size * 1.73, size * 1.77);
}

TEST_F(SketchREQTests, globalAggVeryLowCardinalityIntegers) {
  vector_size_t size = 100;
  auto values =
      makeFlatVector<int32_t>(size, [size](auto row) { return row + size; });

  testGlobalAgg<int>(values, "req(c0, 12, 0.75)", size * 1.73, size * 1.77);
}

TEST_F(SketchREQTests, globalAggIntegersWithDifferentRank) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  testGlobalAgg(values, "req(c0, 12, 0.0)", 0, 3);
  testGlobalAgg(values, "req(c0, 12, 0.5)", 475, 525);
  testGlobalAgg(values, "req(c0, 12, 1.0)", 950, 1050);
}

TEST_F(SketchREQTests, globalAggIntegersWithDifferentK) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  testGlobalAgg(values, "req(c0, 12, 0.0)", 0, 3);
  testGlobalAgg(values, "req(c0, 13, 0.5)", 475, 525);
  testGlobalAgg(values, "req(c0, 11, 1.0)", 950, 1050);
}

TEST_F(SketchREQTests, groupByIntegers) {
  vector_size_t size = 10'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? row * 3 : row * 5; });

  std::unordered_map<int32_t, int32_t> expectedResultMin{
      {0, 15000}, {1, 25000}};
  std::unordered_map<int32_t, int32_t> expectedResultMax{
      {0, 15500}, {1, 26000}};

  testGroupByAgg(
      keys, values, "req(c1, 12, 0.5)", expectedResultMin, expectedResultMax);
}

TEST_F(SketchREQTests, groupByStrings) {
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

  testGroupByAgg(keys, values, "req(c1, 200, 0.5)", expectedMin, expectedMax);
}

TEST_F(SketchREQTests, groupByHighCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  std::unordered_map<int32_t, int32_t> expectedResultMin{{0, 490}, {1, 490}};
  std::unordered_map<int32_t, int32_t> expectedResultMax{{0, 510}, {1, 520}};

  testGroupByAgg(
      keys, values, "req(c1, 12, 0.5)", expectedResultMin, expectedResultMax);
}

TEST_F(SketchREQTests, groupByVeryLowCardinalityIntegers) {
  vector_size_t size = 100;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : row * 2; });

  std::unordered_map<int32_t, int32_t> expectedResultMin{{0, 26}, {1, 90}};
  std::unordered_map<int32_t, int32_t> expectedResultMax{{0, 28}, {1, 110}};

  testGroupByAgg(
      keys, values, "req(c1, 12, 0.5)", expectedResultMin, expectedResultMax);
}

TEST_F(SketchREQTests, groupByAllNulls) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : row % 3; }, nullEvery(2));

  auto op = PlanBuilder()
                .values({makeRowVector({keys, values})})
                .singleAggregation({"c0"}, {"req(c1, 12, 0.5)"})
                .planNode();

  std::unordered_map<int32_t, int32_t> expectedResult{{0, 0}, {1, 1}};
  auto expectedResultMin = toRowVector(expectedResult);

  expectedResult = {{0, 0}, {1, 3}};
  auto expectedResultMax = toRowVector(expectedResult);

  assertQueryBetween(op, expectedResultMin, expectedResultMax);

  op = PlanBuilder()
           .values({makeRowVector({keys, values})})
           .partialAggregation({"c0"}, {"req(c1, 12, 0.75)"})
           .finalAggregation()
           .planNode();
  assertQueryBetween(op, expectedResultMin, expectedResultMax);
}

TEST_F(SketchREQTests, groupByMultiple) {
  vector_size_t size = 1'000;
  auto keys1 = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto keys2 = makeFlatVector<int32_t>(size, [](auto row) { return row % 3; });
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 18; });

  auto plan = PlanBuilder()
                  .values({makeRowVector({keys1, keys2, values})})
                  .partialAggregation({"c0", "c1"}, {"req(c2, 12, 0.5)"})
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

} // namespace bytedance::bolt::aggregate::sketches::test
