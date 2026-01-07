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

#include "SketchTestBase.hpp"
#include "bolt/functions/sketches/SketchFunctionNames.h"
#include "bolt/functions/sketches/SketchHLLEstimate.h"
namespace bytedance::bolt::aggregate::sketches::test {

using HLLSketch = datasketches::hll_sketch;

class SketchHLLUnionTests : public SketchTestBase {
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

  template <typename T>
  std::vector<std::string> getBinarySketchesWithNull(
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
      if (i % szVal == 0) {
        ret.push_back("");
      } else {
        ret.push_back(vec->valueAt(i));
      }
    }

    // release resources task is holding
    result.first->task();

    return ret;
  }

  // This is to test `SELECT hll_union_sketches(c0) FROM T`.
  void verifyHLLUnionSketches(
      const std::vector<std::string>& data,
      double expected) {
    VectorPtr varbinaryVector = toVarBinaryVector(data);

    auto plan = PlanBuilder()
                    .values({makeRowVector({varbinaryVector})})
                    .partialAggregation({}, {"hll_union_sketches(c0) as d0"})
                    .finalAggregation()
                    .planNode();

    auto serializedSketch = readSingleValue(plan).template value<Varbinary>();

    auto sketch = HLLSketch::deserialize(
        serializedSketch.data(), serializedSketch.size());
    double result = sketch.get_estimate();
    EXPECT_BETWEEN(expected * 0.95, result, expected * 1.05);
  }

  // This is to test `SELECT hll_union_sketches(d0) FROM (SELECT
  // hll_union_sketches(c0) AS d0 FROM T)`.
  void verifyHLLNestedUnionSketches(
      const std::vector<std::string>& data,
      double expected) {
    VectorPtr varbinaryVector = toVarBinaryVector(data);

    auto plan = PlanBuilder()
                    .values({makeRowVector({varbinaryVector})})
                    .partialAggregation({}, {"hll_union_sketches(c0) as d0"})
                    .finalAggregation()
                    .singleAggregation({}, {"hll_union_sketches(d0)"})
                    .planNode();

    auto serializedSketch = readSingleValue(plan).template value<Varbinary>();

    auto sketch = HLLSketch::deserialize(
        serializedSketch.data(), serializedSketch.size());
    double result = sketch.get_estimate();
    EXPECT_BETWEEN(expected * 0.95, result, expected * 1.05);
  }

  // This is to test `SELECT c0, hll_estimate(hll_union_sketches(d1)) FROM
  // (SELECT c0, hll_union_sketches(c1) AS d1 FROM T GROUP BY c0) GROUP BY c0`.
  void verifyHLLEstimateUnionSketchesGroupBy(
      const std::vector<std::string>& data,
      int szKey,
      double expectedValue) {
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
};

TEST_F(SketchHLLUnionTests, basic) {
  auto data1 = getBinarySketch<int32_t>(11, datasketches::HLL_4, 17);
  auto data2 = getBinarySketch<int32_t>(13, datasketches::HLL_4, 25, 12);

  // %17 or %25 will give 17, and 25 different elements
  // due to overlap, there will be 37 elements in their union
  verifyHLLUnionSketches({data1, data2}, 37);
}

TEST_F(SketchHLLUnionTests, nestedUnion) {
  auto data1 = getBinarySketch<int32_t>(11, datasketches::HLL_4, 17);
  auto data2 = getBinarySketch<int32_t>(13, datasketches::HLL_4, 25, 12);

  // %17 or %25 will give 17, and 25 different elements
  // due to overlap, there will be 37 elements in their union
  verifyHLLNestedUnionSketches({data1, data2}, 37);
}

TEST_F(SketchHLLUnionTests, emptyBinary) {
  auto data1 = getBinarySketch<int32_t>(11, datasketches::HLL_4, 17);

  // First binary has 17 elements and 2nd is empty. There will be 17 elements in
  // their union
  verifyHLLNestedUnionSketches({data1, ""}, 17);
}

TEST_F(SketchHLLUnionTests, different_lgK) {
  auto data1 = getBinarySketch<int32_t>(10, datasketches::HLL_4, 17);
  auto data2 = getBinarySketch<int32_t>(13, datasketches::HLL_4, 25, 12);

  // %17 or %25 will give 17, and 25 different elements
  // due to overlap, there will be 37 elements in their union
  verifyHLLNestedUnionSketches({data1, data2}, 37);
}

TEST_F(SketchHLLUnionTests, different_type) {
  auto data1 = getBinarySketch<int32_t>(10, datasketches::HLL_4, 17);
  auto data2 = getBinarySketch<double>(13, datasketches::HLL_4, 25, 12);

  // %17 or %25 will give 17, and 25 different elements
  // though they to overlap, due different types, there will be 42 elements in
  // their union
  verifyHLLNestedUnionSketches({data1, data2}, 42);
}

TEST_F(SketchHLLUnionTests, groupBy) {
  auto binarySketches =
      getBinarySketches<int32_t>(12, datasketches::HLL_6, 2, 18);

  std::vector<std::pair<std::string, double>> results;
  std::transform(
      binarySketches.begin(),
      binarySketches.end(),
      std::back_inserter(results),
      [](std::string& str) { return std::make_pair(str, 9); });
  // 18 elements grouped into 2 groups. Ultimately, the estimate should be 18.
  verifyHLLNestedUnionSketches(binarySketches, 18);
}

TEST_F(SketchHLLUnionTests, groupByGroupby) {
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
  // 18 elements grouped into 6 groups. Each group should have 3 elements.
  verifyHLLEstimateUnionSketchesGroupBy(binarySketches, 6, 3);

  // 18 elements grouped into 3 groups. Each group should have 6 elements.
  verifyHLLEstimateUnionSketchesGroupBy(binarySketches, 3, 6);
}

// Test hll_estimate groupByGroupby with null/empty binaries (when key=0)
TEST_F(SketchHLLUnionTests, groupByGroupbyWithNull) {
  registerFunction<functions::SketchHLLEstimate, double, Varbinary>(
      {functions::kHLLEstimate});

  auto binarySketchesWithNull =
      getBinarySketchesWithNull<int32_t>(12, datasketches::HLL_8, 6, 18);

  std::vector<std::pair<std::string, double>> results;
  std::transform(
      binarySketchesWithNull.begin(),
      binarySketchesWithNull.end(),
      std::back_inserter(results),
      [](std::string& str) { return std::make_pair(str, 3); });

  VectorPtr varbinaryVector = toVarBinaryVector(binarySketchesWithNull);
  auto keys = makeFlatVector<int32_t>(
      binarySketchesWithNull.size(), [&](auto row) { return row % 6; });

  auto plan = PlanBuilder()
                  .values({makeRowVector({keys, varbinaryVector})})
                  .project({"c0", "c1"})
                  .partialAggregation({"c0"}, {"hll_union_sketches(c1) as d1"})
                  .localPartition({})
                  .finalAggregation()
                  .project({"c0", "d1"})
                  .filter("c0=0")
                  .singleAggregation({"c0"}, {"hll_union_sketches(d1) as e1"})
                  .project({"e1"})
                  .planNode();

  auto serializedSketch = readSingleValue(plan).template value<Varbinary>();
  EXPECT_EQ(serializedSketch, "");
}
} // namespace bytedance::bolt::aggregate::sketches::test
