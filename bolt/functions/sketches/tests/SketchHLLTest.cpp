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

#include <DataSketches/hll.hpp>
#include <set>

#include "SketchTestBase.hpp"
namespace bytedance::bolt::aggregate::sketches::test {

class SketchHLLTests : public SketchTestBase {};

TEST_F(SketchHLLTests, example) {
  // Publicly documented hll data sketch example, guaranteeing correct linking
  // https://datasketches.apache.org/docs/HLL/HllCppExample.html

  const int lg_k = 11;
  const auto type = datasketches::HLL_4; // this is the default, but explicit
                                         // here for illustration

  // this section generates two sketches with some overlap and serializes them
  // into files
  {
    // 100000 distinct keys
    datasketches::hll_sketch sketch1(
        lg_k, type); // type is optional, defaults to HLL_4
    for (int key = 0; key < 100000; key++)
      sketch1.update(key);
    std::ofstream os1("hll_sketch1.bin");
    sketch1.serialize_compact(os1);

    // 100000 distinct keys
    datasketches::hll_sketch sketch2(
        lg_k, type); // type is optional, defaults to HLL_4
    for (int key = 50000; key < 150000; key++)
      sketch2.update(key);
    std::ofstream os2("hll_sketch2.bin");
    sketch2.serialize_compact(os2);
  }

  // this section deserializes the sketches, produces union and prints the
  // result
  {
    std::ifstream is1("hll_sketch1.bin");
    datasketches::hll_sketch sketch1 =
        datasketches::hll_sketch::deserialize(is1);

    std::ifstream is2("hll_sketch2.bin");
    datasketches::hll_sketch sketch2 =
        datasketches::hll_sketch::deserialize(is2);

    datasketches::hll_union u(lg_k);
    u.update(sketch1);
    u.update(sketch2);
    datasketches::hll_sketch sketch =
        u.get_result(type); // type is optional, defaults to HLL_4

    EXPECT_EQ((int)sketch.get_estimate(), 152040);
    EXPECT_EQ((int)sketch.get_lower_bound(2), 145233);
    EXPECT_EQ((int)sketch.get_upper_bound(2), 159184);
  }
}

TEST_F(SketchHLLTests, singleAggregation) {
  vector_size_t size = 10'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 17; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .singleAggregation({}, {"hll(c0, 11, 0)"})
                .planNode();

  int result = (int)readSingleValue(op).value<double>();
  EXPECT_EQ(result, 17);
}

TEST_F(SketchHLLTests, basic) {
  vector_size_t size = 10'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 17; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .partialAggregation({}, {"hll(c0, 11, 0)"})
                .finalAggregation()
                .planNode();
  int result = (int)readSingleValue(op).value<double>();
  EXPECT_EQ(result, 17);
}

TEST_F(SketchHLLTests, groupByIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? row % 17 : row % 21 + 100; });

  std::unordered_map<int32_t, double> expectedResultMin{{0, 16.9}, {1, 20.8}};
  std::unordered_map<int32_t, double> expectedResultMax{{0, 17.1}, {1, 21.2}};

  testGroupByAgg(
      keys, values, "hll(c1, 11, 0)", expectedResultMin, expectedResultMax);
}

TEST_F(SketchHLLTests, groupByStrings) {
  vector_size_t size = 1'000;
  double fSz = kFruits.size();
  double vSz = kVegetables.size();

  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<StringView>(size, [&](auto row) {
    return StringView(
        row % 2 == 0 ? kFruits[row % kFruits.size()]
                     : kVegetables[row % kVegetables.size()]);
  });

  std::unordered_map<int32_t, double> expectedMin{{0, fSz - 1}, {1, vSz - 1}};
  std::unordered_map<int32_t, double> expectedMax{{0, fSz + 1}, {1, vSz + 1}};

  testGroupByAgg(keys, values, "hll(c1, 11, 0)", expectedMin, expectedMax);
}

TEST_F(SketchHLLTests, groupByHighCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  std::unordered_map<int32_t, double> expectedResultMin{{0, 495}, {1, 495}};
  std::unordered_map<int32_t, double> expectedResultMax{{0, 511}, {1, 511}};

  testGroupByAgg(
      keys, values, "hll(c1, 11, 0)", expectedResultMin, expectedResultMax);
}

TEST_F(SketchHLLTests, groupByVeryLowCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : row % 3; });

  std::unordered_map<int32_t, double> expectedResultMin{{0, 1.0}, {1, 2.8}};
  std::unordered_map<int32_t, double> expectedResultMax{{0, 1.0}, {1, 3.2}};

  testGroupByAgg(
      keys, values, "hll(c1, 11, 0)", expectedResultMin, expectedResultMax);
}

TEST_F(SketchHLLTests, groupByAllNulls) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : row % 3; }, nullEvery(2));

  std::unordered_map<int32_t, double> expectedResultsMin{{0, 0}, {1, 2.9}};
  std::unordered_map<int32_t, double> expectedResultsMax{{0, 1.1}, {1, 3.1}};
  RowVectorPtr expectedMin = toRowVector(expectedResultsMin);
  RowVectorPtr expectedMax = toRowVector(expectedResultsMax);

  auto op = PlanBuilder()
                .values({makeRowVector({keys, values})})
                .singleAggregation({"c0"}, {"hll(c1, 11, 0)"})
                .planNode();
  assertQueryBetween(op, expectedMin, expectedMax);

  op = PlanBuilder()
           .values({makeRowVector({keys, values})})
           .partialAggregation({"c0"}, {"hll(c1, 11, 0)"})
           .finalAggregation()
           .planNode();
  assertQueryBetween(op, expectedMin, expectedMax);
}

TEST_F(SketchHLLTests, globalAggIntegers) {
  vector_size_t size = 1'000;
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 17; });

  testGlobalAgg(values, "hll(c0, 11, 0)", 16.9, 17.1);
}

TEST_F(SketchHLLTests, globalAggStrings) {
  vector_size_t size = 1'000;

  auto values = makeFlatVector<StringView>(size, [&](auto row) {
    return StringView(kFruits[row % kFruits.size()]);
  });

  testGlobalAgg(
      values, "hll(c0, 11, 0)", kFruits.size() - 0.5, kFruits.size() + 0.5);
}

TEST_F(SketchHLLTests, globalAggHighCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  testGlobalAgg<double>(values, "hll(c0, 11, 0)", 950, 1'001);
}

TEST_F(SketchHLLTests, globalAggVeryLowCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto /*row*/) { return 27; });

  testGlobalAgg<double>(values, "hll(c0, 11, 0)", 1, 1);
}

TEST_F(SketchHLLTests, globalAggIntegersWithDifferentLogK) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  testGlobalAgg<double>(values, "hll(c0, 11, 0)", 950, 1050);
  testGlobalAgg<double>(values, "hll(c0, 10, 0)", 950, 1050);
  testGlobalAgg<double>(values, "hll(c0, 12, 0)", 975, 1025);
}

TEST_F(SketchHLLTests, groupByMultiple) {
  vector_size_t size = 1'000;
  auto keys1 = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto keys2 = makeFlatVector<int32_t>(size, [](auto row) { return row % 3; });
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 18; });

  auto plan = PlanBuilder()
                  .values({makeRowVector({keys1, keys2, values})})
                  .partialAggregation({"c0", "c1"}, {"hll(c2, 11, 0)"})
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

TEST_F(SketchHLLTests, stableEstimateShuffleHLL) {
  const int numShuffles = 20;

  std::random_device rd;
  std::mt19937 g(rd());
  std::uniform_int_distribution<> distrib(10000, 100000);
  const int distinctCount = distrib(g);

  std::vector<int32_t> data;
  for (int i = 0; i < distinctCount; ++i) {
    data.push_back(i);
    data.push_back(i);
  }
  const int numItems = data.size();

  double firstResult = -1.0;

  for (int i = 0; i < numShuffles; ++i) {
    std::shuffle(data.begin(), data.end(), g);

    auto dataVector =
        makeFlatVector<int32_t>(numItems, [&](auto row) { return data[row]; });

    // Test with isStable=true
    auto plan = PlanBuilder()
                    .values({makeRowVector({dataVector})})
                    .singleAggregation({}, {"hll(c0, 15, 2, true)"})
                    .planNode();

    auto result = readSingleValue(plan).value<double>();
    if (firstResult < 0) {
      firstResult = result;
    } else {
      EXPECT_EQ(result, firstResult);
    }
  }
}
} // namespace bytedance::bolt::aggregate::sketches::test
