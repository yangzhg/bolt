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
#include <iostream>
#include <random>
#include "bolt/common/encode/Base64.h"

#include <string>
#include "SketchTestBase.hpp"
#include "TestSketchEstimateData.h"
#include "bolt/functions/sketches/SketchFunctionNames.h"
#include "bolt/functions/sketches/SketchHLLEstimate.h"
namespace bytedance::bolt::aggregate::sketches::test {

using HLLSketch = datasketches::hll_sketch;

class SketchHLLEstimateTests : public SketchTestBase {
 public:
  template <typename T>
  std::string getBinarySketch(
      int lgK,
      datasketches::target_hll_type type,
      int mod,
      int low = 0) {
    vector_size_t size = 10'000;
    auto values =
        makeFlatVector<T>(size, [&](auto row) { return row % mod + low; });

    auto op =
        PlanBuilder()
            .values({makeRowVector({values})})
            .partialAggregation({}, {fmt::format("hll(c0, {}, {})", lgK, type)})
            .planNode();

    return readSingleValue(op).template value<Varbinary>();
  }

  template <typename T>
  std::string getBinarySketchByDataInput(
      int lgK,
      datasketches::target_hll_type type,
      const std::vector<T>& data) {
    vector_size_t size = data.size();

    if (size == 0) {
      auto emptyValues = makeFlatVector<T>(1, [&](auto row) { return T(); });
      auto emptyOp = PlanBuilder()
                         .values({makeRowVector({emptyValues})})
                         .partialAggregation(
                             {}, {fmt::format("hll(c0, {}, {})", lgK, type)})
                         .planNode();
      return readSingleValue(emptyOp).template value<Varbinary>();
    }

    auto values = makeFlatVector<T>(size, [&](auto row) { return data[row]; });

    auto op =
        PlanBuilder()
            .values({makeRowVector({values})})
            .partialAggregation({}, {fmt::format("hll(c0, {}, {})", lgK, type)})
            .planNode();

    return readSingleValue(op).template value<Varbinary>();
  }

  template <typename T>
  std::vector<std::string> getBinarySketches(
      int lgK,
      datasketches::target_hll_type type,
      int szKey,
      int szVal) {
    vector_size_t size = 10'000;
    auto keys =
        makeFlatVector<int32_t>(size, [&](auto row) { return row % szKey; });
    auto values =
        makeFlatVector<T>(size, [&](auto row) { return row % szVal; });

    auto plan = PlanBuilder()
                    .values({makeRowVector({keys, values})})
                    .partialAggregation(
                        {"c0"}, {fmt::format("hll(c1, {}, {})", lgK, type)})
                    .planNode();

    CursorParameters params;
    params.planNode = plan;
    auto result = readCursor(params, [](Task*) {});
    auto actual = result.second;

    std::vector<std::string> ret;
    auto vec = actual[0]->childAt(1)->template as<FlatVector<StringView>>();
    for (int i = 0; i < vec->size(); i++) {
      ret.push_back(vec->valueAt(i));
    }

    // release resources task is holding
    result.first->task();

    return ret;
  }

  // Decode base64 to sketches
  std::vector<uint8_t> base64Decode(const std::string& encoded_string) {
    std::string decoded_string = encoding::Base64::decode(encoded_string);
    std::vector<uint8_t> decoded_vector(
        decoded_string.begin(), decoded_string.end());
    return decoded_vector;
  }

  // This is to test `SELECT hll_estimate(c0) FROM T`.
  void verifyHLLEstimateSketches(
      const std::vector<std::string>& data,
      double expected) {
    VectorPtr varbinaryVector = toVarBinaryVector(data);

    auto plan = PlanBuilder()
                    .values({makeRowVector({varbinaryVector})})
                    .project({"hll_estimate(c0)"})
                    .planNode();

    auto result = readSingleValue(plan).value<double>();
    EXPECT_BETWEEN(expected * 0.95, result, expected * 1.05);
  }

  void verifyStableHLLEstimateSketches(
      const std::vector<std::string>& data,
      double expected) {
    VectorPtr varbinaryVector = toVarBinaryVector(data);

    auto plan = PlanBuilder()
                    .values({makeRowVector({varbinaryVector})})
                    .project({"hll_estimate(c0, true)"})
                    .planNode();

    auto result = readSingleValue(plan).value<double>();
    EXPECT_EQ(result, expected);
  }

  // This is to test `SELECT hll_estimate(hll_union_sketches(c0) FROM
  // T`.
  void verifyHLLEstimateUnionSketches(
      const std::vector<std::string>& data,
      double expected) {
    VectorPtr varbinaryVector = toVarBinaryVector(data);

    auto plan = PlanBuilder()
                    .values({makeRowVector({varbinaryVector})})
                    .partialAggregation({}, {"hll_union_sketches(c0) as d0"})
                    .finalAggregation()
                    .project({"hll_estimate(d0)"})
                    .planNode();

    auto result = readSingleValue(plan).value<double>();
    EXPECT_BETWEEN(expected * 0.95, result, expected * 1.05);
  }

  // This is to test `SELECT hll_estimate(hll_union_sketches(c0), true) FROM T`.
  void verifyStableHLLEstimateUnionSketches(
      const std::vector<std::string>& data,
      double expected) {
    VectorPtr varbinaryVector = toVarBinaryVector(data);

    auto plan = PlanBuilder()
                    .values({makeRowVector({varbinaryVector})})
                    .partialAggregation({}, {"hll_union_sketches(c0) as d0"})
                    .finalAggregation()
                    .project({"hll_estimate(d0, true)"})
                    .planNode();

    auto result = readSingleValue(plan).value<double>();
    EXPECT_EQ(result, expected);
  }

  // This is to test `SELECT c0, hll_estimate(hll_union_sketches(d1))
  // FROM (SELECT c0, hll_union_sketches(c1) AS d1 FROM T GROUP BY c0) GROUP BY
  // c0`.
  void verifyHLLEstimateUnionSketchesGroupBy(
      const std::vector<std::string>& data,
      int szKey,
      int expectedValue) {
    VectorPtr varbinaryVector = toVarBinaryVector(data);
    auto keys = makeFlatVector<int32_t>(
        data.size(), [&](auto row) { return row % szKey; });

    auto plan =
        PlanBuilder()
            .values({makeRowVector({keys, varbinaryVector})})
            .project({"c0", "c1"})
            .partialAggregation({"c0"}, {"hll_union_sketches(c1) as d1"})
            .localPartition({})
            .finalAggregation()
            .project({"c0", "d1"})
            .singleAggregation({"c0"}, {"hll_union_sketches(d1) as e1"})
            .project({"c0", "hll_estimate(e1)"})
            .planNode();

    std::unordered_map<int32_t, double> expectedResultMin =
        toKeyValueMap(szKey, expectedValue * 0.95);
    std::unordered_map<int32_t, double> expectedResultMax =
        toKeyValueMap(szKey, expectedValue * 1.05);

    assertQueryBetween(
        plan, toRowVector(expectedResultMin), toRowVector(expectedResultMax));
  }

  // This is to test `SELECT c0, hll_estimate(hll_union_sketches(d1), true) FROM
  // (SELECT c0, hll_union_sketches(c1) AS d1 FROM T GROUP BY c0) GROUP BY c0`.
  void verifyStableHLLEstimateUnionSketchesGroupBy(
      const std::vector<std::string>& data,
      int szKey,
      int expectedValue) {
    VectorPtr varbinaryVector = toVarBinaryVector(data);
    auto keys = makeFlatVector<int32_t>(
        data.size(), [&](auto row) { return row % szKey; });

    auto plan =
        PlanBuilder()
            .values({makeRowVector({keys, varbinaryVector})})
            .project({"c0", "c1"})
            .partialAggregation({"c0"}, {"hll_union_sketches(c1) as d1"})
            .localPartition({})
            .finalAggregation()
            .project({"c0", "d1"})
            .singleAggregation({"c0"}, {"hll_union_sketches(d1) as e1"})
            .project({"c0", "hll_estimate(e1, true)"})
            .planNode();

    std::unordered_map<int32_t, double> expectedResult =
        toKeyValueMap(szKey, expectedValue);

    assertQuery(plan, toRowVector(expectedResult));
  }
};

TEST_F(SketchHLLEstimateTests, basic) {
  registerFunction<functions::SketchHLLEstimate, double, Varbinary>(
      {functions::kHLLEstimate});
  auto data1 = getBinarySketch<int32_t>(11, datasketches::HLL_4, 17);
  verifyHLLEstimateSketches({data1}, 17);
}

TEST_F(SketchHLLEstimateTests, estimateUnionSketches) {
  registerFunction<functions::SketchHLLEstimate, double, Varbinary>(
      {functions::kHLLEstimate});
  auto data1 = getBinarySketch<int32_t>(11, datasketches::HLL_4, 17);
  auto data2 = getBinarySketch<int32_t>(13, datasketches::HLL_4, 25, 12);

  // %17 or %25 will give 17, and 25 different elements
  // due to overlap, there will be 37 elements in their union
  verifyHLLEstimateUnionSketches({data1, data2}, 37);
}

TEST_F(SketchHLLEstimateTests, estimateUnionGroupByGroupBy) {
  registerFunction<functions::SketchHLLEstimate, double, Varbinary>(
      {functions::kHLLEstimate});

  auto binarySketches =
      getBinarySketches<int32_t>(12, datasketches::HLL_8, 6, 18);

  std::vector<std::pair<std::string, double>> results;
  std::transform(
      binarySketches.begin(),
      binarySketches.end(),
      std::back_inserter(results),
      [](std::string& str) { return std::make_pair(str, 3); });
  verifyHLLEstimateUnionSketchesGroupBy(binarySketches, 6, 3);
  verifyHLLEstimateUnionSketchesGroupBy(binarySketches, 3, 6);
}

TEST_F(SketchHLLEstimateTests, basicStableEstimate) {
  registerFunction<functions::SketchHLLEstimate, double, Varbinary, bool>(
      {functions::kHLLEstimate});
  auto data1 = getBinarySketch<int32_t>(11, datasketches::HLL_4, 17);
  verifyStableHLLEstimateSketches({data1}, 17);
}

TEST_F(SketchHLLEstimateTests, stableEstimateUnionSketches) {
  registerFunction<functions::SketchHLLEstimate, double, Varbinary, bool>(
      {functions::kHLLEstimate});
  auto data1 = getBinarySketch<int32_t>(11, datasketches::HLL_4, 17);
  auto data2 = getBinarySketch<int32_t>(13, datasketches::HLL_4, 25, 12);

  // %17 or %25 will give 17, and 25 different elements
  // due to overlap, there will be 37 elements in their union
  verifyStableHLLEstimateUnionSketches({data1, data2}, 37);
}

TEST_F(SketchHLLEstimateTests, stableEstimateUnionGroupByGroupBy) {
  registerFunction<functions::SketchHLLEstimate, double, Varbinary, bool>(
      {functions::kHLLEstimate});

  auto binarySketches =
      getBinarySketches<int32_t>(12, datasketches::HLL_8, 6, 18);

  std::vector<std::pair<std::string, double>> results;
  std::transform(
      binarySketches.begin(),
      binarySketches.end(),
      std::back_inserter(results),
      [](std::string& str) { return std::make_pair(str, 3); });
  verifyStableHLLEstimateUnionSketchesGroupBy(binarySketches, 6, 3);
  verifyStableHLLEstimateUnionSketchesGroupBy(binarySketches, 3, 6);
}

TEST_F(SketchHLLEstimateTests, stableEstimateSchuffleUnionSketches) {
  registerFunction<functions::SketchHLLEstimate, double, Varbinary, bool>(
      {functions::kHLLEstimate});

  // Shuffle times
  const int numShuffles = 200;
  const double expectedEstimate = 483;

  std::random_device rd;
  std::mt19937 g(rd());

  for (int i = 0; i < numShuffles; ++i) {
    // shuffle the order of sketchEstimateData
    std::vector<std::string> shuffledSketchEstimateData =
        bytedance::bolt::aggregate::sketches::test::sketchEstimateData;
    std::shuffle(
        shuffledSketchEstimateData.begin(),
        shuffledSketchEstimateData.end(),
        g);

    // Base64 decode
    std::vector<std::vector<uint8_t>> decodedSketches;
    for (const auto& sketchStr : shuffledSketchEstimateData) {
      decodedSketches.push_back(base64Decode(sketchStr));
    }

    // Convert decodedSketches to std::vector<std::string>
    std::vector<std::string> stringSketches;
    for (const auto& sketch : decodedSketches) {
      stringSketches.emplace_back(sketch.begin(), sketch.end());
    }

    VectorPtr varbinaryVector = toVarBinaryVector(stringSketches);

    // Build query plan
    auto plan = PlanBuilder()
                    .values({makeRowVector({varbinaryVector})})
                    .partialAggregation({}, {"hll_union_sketches(c0) as d0"})
                    .finalAggregation()
                    .project({"hll_estimate(d0, true)"})
                    .planNode();

    auto result = readSingleValue(plan).value<double>();
    EXPECT_EQ(result, expectedEstimate);

    // test stable hll_sketch
    auto plan2 = PlanBuilder()
                     .values({makeRowVector({varbinaryVector})})
                     .partialAggregation({}, {"hll_sketch(c0, true) as d0"})
                     .finalAggregation()
                     .planNode();
    auto configs = std::unordered_map<std::string, std::string>{
        {core::QueryConfig::kHllSketchRounded, "true"}};
    auto result2 = readSingleValue(plan2, 1, configs).value<double>();
    EXPECT_EQ(result2, expectedEstimate);
  }
}

TEST_F(SketchHLLEstimateTests, couponShuffleListStableEstimateWithDuplicate) {
  registerFunction<functions::SketchHLLEstimate, double, Varbinary, bool>(
      {functions::kHLLEstimate});

  int maxShuffleTimes = 100;

  std::random_device rd;
  std::mt19937 g(rd());

  std::vector<int32_t> dataInput;
  for (int32_t i = 1; i <= 7; ++i) {
    dataInput.push_back(i);
    dataInput.push_back(i);
  }

  for (int i = 0; i < maxShuffleTimes; i++) {
    std::shuffle(dataInput.begin(), dataInput.end(), g);
    auto data1 =
        getBinarySketchByDataInput<int32_t>(11, datasketches::HLL_4, dataInput);
    verifyStableHLLEstimateSketches({data1}, 7);
  }

  dataInput.push_back(8);
  dataInput.push_back(8);

  for (int i = 0; i < maxShuffleTimes; i++) {
    std::shuffle(dataInput.begin(), dataInput.end(), g);
    auto data2 =
        getBinarySketchByDataInput<int32_t>(11, datasketches::HLL_4, dataInput);
    verifyStableHLLEstimateSketches({data2}, 8);
  }

  for (int32_t i = 9; i <= 27; ++i) {
    dataInput.push_back(i);
    dataInput.push_back(i);
  }

  for (int i = 0; i < maxShuffleTimes; i++) {
    std::shuffle(dataInput.begin(), dataInput.end(), g);
    auto data3 =
        getBinarySketchByDataInput<int32_t>(11, datasketches::HLL_4, dataInput);
    verifyStableHLLEstimateSketches({data3}, 27);
  }
}

TEST_F(SketchHLLEstimateTests, hllShuffleUnionStableEstimate) {
  registerFunction<functions::SketchHLLEstimate, double, Varbinary, bool>(
      {functions::kHLLEstimate});

  std::vector<int32_t> dataInput;
  int maxShuffleTimes = 100;
  std::random_device rd;
  std::mt19937 g(rd());

  for (int32_t i = 1; i <= 15; ++i) {
    dataInput.push_back(i);
  }
  for (int i = 0; i < maxShuffleTimes; i++) {
    std::shuffle(dataInput.begin(), dataInput.end(), g);
    auto data1 =
        getBinarySketchByDataInput<int32_t>(11, datasketches::HLL_4, dataInput);
    verifyStableHLLEstimateSketches({data1}, 15);
  }

  for (int32_t i = 16; i <= 100; ++i) {
    dataInput.push_back(i);
  }
  for (int i = 0; i < maxShuffleTimes; i++) {
    std::shuffle(dataInput.begin(), dataInput.end(), g);
    auto data2 =
        getBinarySketchByDataInput<int32_t>(11, datasketches::HLL_4, dataInput);
    verifyStableHLLEstimateSketches({data2}, 100);
  }
}

} // namespace bytedance::bolt::aggregate::sketches::test
