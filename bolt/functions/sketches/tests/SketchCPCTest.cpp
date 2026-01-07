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

#include <DataSketches/cpc_sketch.hpp>
#include <DataSketches/cpc_union.hpp>

#include "SketchTestBase.hpp"
namespace bytedance::bolt::aggregate::sketches::test {

class SketchCPCTests : public SketchTestBase {};

TEST_F(SketchCPCTests, example) {
  // Publicly documented cpc data sketch example, guaranteeing correct linking
  // https://datasketches.apache.org/docs/CPC/CpcCppExample.html

  const int lg_k = 10;

  // this section generates two sketches with some overlap and serializes them
  // into files
  {
    // 100000 distinct keys
    datasketches::cpc_sketch sketch1(lg_k);
    for (int key = 0; key < 100000; key++)
      sketch1.update(key);
    std::ofstream os1("cpc_sketch1.bin");
    sketch1.serialize(os1);

    // 100000 distinct keys
    datasketches::cpc_sketch sketch2(lg_k);
    for (int key = 50000; key < 150000; key++)
      sketch2.update(key);
    std::ofstream os2("cpc_sketch2.bin");
    sketch2.serialize(os2);
  }

  // this section deserializes the sketches, produces union and prints the
  // result
  {
    std::ifstream is1("cpc_sketch1.bin");
    datasketches::cpc_sketch sketch1 =
        datasketches::cpc_sketch::deserialize(is1);

    std::ifstream is2("cpc_sketch2.bin");
    datasketches::cpc_sketch sketch2 =
        datasketches::cpc_sketch::deserialize(is2);

    datasketches::cpc_union u(lg_k);
    u.update(sketch1);
    u.update(sketch2);
    datasketches::cpc_sketch sketch = u.get_result();

    EXPECT_EQ((int)sketch.get_estimate(), 149796);
    EXPECT_EQ((int)sketch.get_lower_bound(2), 143416);
    EXPECT_EQ((int)sketch.get_upper_bound(2), 156397);
  }
}

TEST_F(SketchCPCTests, singleAggregation) {
  vector_size_t size = 10'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 17; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .singleAggregation({}, {"cpc(c0, 11)"})
                .planNode();

  int result = (int)readSingleValue(op).value<double>();
  EXPECT_EQ(result, 17);
}

TEST_F(SketchCPCTests, basic) {
  vector_size_t size = 10'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 17; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .partialAggregation({}, {"cpc(c0, 11)"})
                .finalAggregation()
                .planNode();
  int result = (int)readSingleValue(op).value<double>();
  EXPECT_EQ(result, 17);
}

TEST_F(SketchCPCTests, basicPartial) {
  vector_size_t size = 10'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 17; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .partialAggregation({}, {"cpc(c0, 11)"})
                .planNode();
  auto result = readSingleValue(op).value<Varbinary>();
  std::string expectedPrefix = "\b\U00000001\U00000010\v";
  EXPECT_EQ(result.substr(0, expectedPrefix.size()), expectedPrefix);
}

TEST_F(SketchCPCTests, groupByIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? row % 17 : row % 21 + 100; });

  std::unordered_map<int32_t, double> expectedResultMin = {{0, 15}, {1, 19}};
  std::unordered_map<int32_t, double> expectedResultMax = {{0, 19}, {1, 23}};

  testGroupByAgg(
      keys, values, "cpc(c1, 11)", expectedResultMin, expectedResultMax);
}

TEST_F(SketchCPCTests, groupByMultiple) {
  vector_size_t size = 1'000;
  auto keys1 = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto keys2 = makeFlatVector<int32_t>(size, [](auto row) { return row % 3; });
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 18; });

  auto plan = PlanBuilder()
                  .values({makeRowVector({keys1, keys2, values})})
                  .partialAggregation({"c0", "c1"}, {"cpc(c2, 11)"})
                  .finalAggregation()
                  .planNode();

  CursorParameters params;
  params.planNode = plan;
  auto result = readCursor(params, [](Task*) {});
  auto actual = result.second;

  auto col3 = actual[0]->childAt(2)->template as<FlatVector<double>>();
  EXPECT_EQ(col3->size(), 2 * 3);
  for (int i = 0; i < col3->size(); i++) {
    EXPECT_BETWEEN(3 * 0.95, col3->valueAt(i), 3 * 1.05);
  }

  result.first->task();
}

TEST_F(SketchCPCTests, groupByStrings) {
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

  testGroupByAgg(keys, values, "cpc(c1, 11)", expectedMin, expectedMax);
}

TEST_F(SketchCPCTests, groupByHighCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  std::unordered_map<int32_t, double> expectedResultMin{{0, 490}, {1, 490}};
  std::unordered_map<int32_t, double> expectedResultMax{{0, 510}, {1, 500}};

  testGroupByAgg(
      keys, values, "cpc(c1, 11)", expectedResultMin, expectedResultMax);
}

TEST_F(SketchCPCTests, groupByVeryLowCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : row % 3; });

  std::unordered_map<int32_t, double> expectedResultMin{{0, 1.0}, {1, 2.8}};
  std::unordered_map<int32_t, double> expectedResultMax{{0, 1.0}, {1, 3.2}};

  testGroupByAgg(
      keys, values, "cpc(c1, 11)", expectedResultMin, expectedResultMax);
}

TEST_F(SketchCPCTests, groupByAllNulls) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : 35; }, nullEvery(2));

  std::unordered_map<int32_t, double> expectedResult = {{0, 0}, {1, 1}};
  auto expected = toRowVector(expectedResult);
  auto op = PlanBuilder()
                .values({makeRowVector({keys, values})})
                .singleAggregation({"c0"}, {"cpc(c1, 11)"})
                .planNode();
  assertQuery(op, expected);

  op = PlanBuilder()
           .values({makeRowVector({keys, values})})
           .partialAggregation({"c0"}, {"cpc(c1, 11)"})
           .finalAggregation()
           .planNode();
  assertQuery(op, expected);
}

TEST_F(SketchCPCTests, globalAggIntegers) {
  vector_size_t size = 1'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 17; });

  testGlobalAgg(values, "cpc(c0, 11)", 16.9, 17.1);
}

TEST_F(SketchCPCTests, globalAggStrings) {
  vector_size_t size = 1'000;

  auto values = makeFlatVector<StringView>(size, [&](auto row) {
    return StringView(kFruits[row % kFruits.size()]);
  });

  testGlobalAgg(
      values, "cpc(c0, 11)", kFruits.size() - 0.5, kFruits.size() + 0.5);
}

TEST_F(SketchCPCTests, globalAggHighCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  testGlobalAgg<double>(values, "cpc(c0, 11)", 995, 1'000);
}

TEST_F(SketchCPCTests, globalAggVeryLowCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto /*row*/) { return 27; });

  testGlobalAgg<double>(values, "cpc(c0, 11)", 1, 1);
}

TEST_F(SketchCPCTests, globalAggIntegersWithDifferentLogK) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  testGlobalAgg<double>(values, "cpc(c0, 11)", 975, 1025);
  testGlobalAgg<double>(values, "cpc(c0, 10)", 950, 1050);
  testGlobalAgg<double>(values, "cpc(c0, 12)", 995, 1005);
}

} // namespace bytedance::bolt::aggregate::sketches::test
