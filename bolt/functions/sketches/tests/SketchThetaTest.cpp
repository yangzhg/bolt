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

#include <DataSketches/theta_sketch.hpp>
#include <DataSketches/theta_union.hpp>

#include "SketchTestBase.hpp"
namespace bytedance::bolt::aggregate::sketches::test {

class SketchThetaTests : public SketchTestBase {};

TEST_F(SketchThetaTests, example) {
  // Publicly documented hll data sketch example, guaranteeing correct linking
  auto sketch1 = datasketches::update_theta_sketch::builder()
                     .set_lg_k(11)
                     .set_p(1.0f)
                     .build();
  int value = 0;
  for (int i = 0; i < 1000; i++)
    sketch1.update(value++);
  auto bytes1 = sketch1.compact().serialize();

  auto sketch2 = datasketches::update_theta_sketch::builder()
                     .set_lg_k(11)
                     .set_p(1)
                     .build();
  value = 500;
  for (int i = 0; i < 1000; i++)
    sketch2.update(value++);
  auto bytes2 = sketch2.compact().serialize();

  auto u = datasketches::theta_union::builder().build();
  u.update(datasketches::wrapped_compact_theta_sketch::wrap(
      bytes1.data(), bytes1.size()));
  u.update(datasketches::wrapped_compact_theta_sketch::wrap(
      bytes2.data(), bytes2.size()));
  datasketches::compact_theta_sketch sketch3 = u.get_result();
  EXPECT_EQ(sketch3.is_empty(), false);
  EXPECT_EQ(sketch3.is_estimation_mode(), false);
  EXPECT_EQ(sketch3.get_estimate(), 1500.0);
}

TEST_F(SketchThetaTests, basic) {
  vector_size_t size = 10'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 17; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .partialAggregation({}, {"theta(c0, 11, 1.0)"})
                .finalAggregation()
                .planNode();
  int result = (int)readSingleValue(op).value<double>();
  EXPECT_EQ(result, 17);
}

TEST_F(SketchThetaTests, singleAggregation) {
  vector_size_t size = 10'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 17; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .singleAggregation({}, {"theta(c0, 11, 1.0)"})
                .planNode();

  int result = (int)readSingleValue(op).value<double>();
  EXPECT_EQ(result, 17);
}

TEST_F(SketchThetaTests, globalAggIntegers) {
  vector_size_t size = 1'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 17; });

  testGlobalAgg(values, "theta(c0, 11, 1.0)", 16.9, 17.1);
}

TEST_F(SketchThetaTests, globalAggStrings) {
  vector_size_t size = 1'000;

  auto values = makeFlatVector<StringView>(size, [&](auto row) {
    return StringView(kFruits[row % kFruits.size()]);
  });

  testGlobalAgg(
      values, "theta(c0, 12, 1.0)", kFruits.size() - 0.5, kFruits.size() + 0.5);
}

TEST_F(SketchThetaTests, groupByIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? row % 17 : row % 21 + 100; });

  std::unordered_map<int32_t, double> expectedResultMin{{0, 15}, {1, 19}};
  std::unordered_map<int32_t, double> expectedResultMax{{0, 19}, {1, 23}};

  testGroupByAgg(
      keys, values, "theta(c1, 12, 1.0)", expectedResultMin, expectedResultMax);
}

TEST_F(SketchThetaTests, groupByStrings) {
  vector_size_t size = 1'000;
  double fSz = kFruits.size();
  double vSz = kVegetables.size();

  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<StringView>(size, [&](auto row) {
    return StringView(
        row % 2 == 0 ? kFruits[row % kFruits.size()]
                     : kVegetables[row % kVegetables.size()]);
  });

  std::unordered_map<int32_t, double> expectedMin{{0, fSz - 2}, {1, vSz - 2}};
  std::unordered_map<int32_t, double> expectedMax{{0, fSz + 2}, {1, vSz + 2}};

  testGroupByAgg(keys, values, "theta(c1, 12, 1.0)", expectedMin, expectedMax);
}

TEST_F(SketchThetaTests, groupByHighCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  std::unordered_map<int32_t, double> expectedResultMin{{0, 490}, {1, 490}};
  std::unordered_map<int32_t, double> expectedResultMax{{0, 510}, {1, 500}};

  testGroupByAgg(
      keys, values, "theta(c1, 12, 1.0)", expectedResultMin, expectedResultMax);
}

TEST_F(SketchThetaTests, groupByVeryLowCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : row % 3; });

  std::unordered_map<int32_t, double> expectedResultMin{{0, 1.0}, {1, 2.8}};
  std::unordered_map<int32_t, double> expectedResultMax{{0, 1.0}, {1, 3.2}};

  testGroupByAgg(
      keys, values, "theta(c1, 12, 1.0)", expectedResultMin, expectedResultMax);
}

TEST_F(SketchThetaTests, groupByAllNulls) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : 35; }, nullEvery(2));

  std::unordered_map<int32_t, double> expectedResult = {{0, 0}, {1, 1}};
  auto expected = toRowVector(expectedResult);
  auto op = PlanBuilder()
                .values({makeRowVector({keys, values})})
                .singleAggregation({"c0"}, {"theta(c1, 12, 1.0)"})
                .planNode();
  assertQuery(op, expected);

  op = PlanBuilder()
           .values({makeRowVector({keys, values})})
           .partialAggregation({"c0"}, {"theta(c1, 12, 1.0)"})
           .finalAggregation()
           .planNode();
  assertQuery(op, expected);
}

TEST_F(SketchThetaTests, groupByMultiple) {
  vector_size_t size = 1'000;
  auto keys1 = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto keys2 = makeFlatVector<int32_t>(size, [](auto row) { return row % 3; });
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row % 6; });

  auto plan = PlanBuilder()
                  .values({makeRowVector({keys1, keys2, values})})
                  .partialAggregation({"c0", "c1"}, {"theta(c2, 12, 1.0)"})
                  .finalAggregation()
                  .planNode();

  CursorParameters params;
  params.planNode = plan;
  auto result = readCursor(params, [](Task*) {});
  auto actual = result.second;

  auto col3 = actual[0]->childAt(2)->template as<FlatVector<double>>();
  EXPECT_EQ(col3->size(), 2 * 3);
  for (int i = 0; i < col3->size(); i++) {
    EXPECT_EQ(col3->valueAt(i), 1);
  }

  result.first->task();
}

} // namespace bytedance::bolt::aggregate::sketches::test
