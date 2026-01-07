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

#include "bolt/duckdb/conversion/DuckConversion.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/QueryAssertions.h"
#include "bolt/vector/VectorTypeUtils.h"
namespace bytedance::bolt::aggregate::sketches::test {

using HLLSketch = datasketches::hll_sketch;

class SketchHLLSketchTests : public SketchTestBase {
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

  void verifySketchUnion(
      const std::vector<std::pair<std::string, double>>& results,
      uint8_t lgK,
      double expectedUnionResult) {
    datasketches::hll_union u(lgK);

    for (const auto& result : results) {
      auto [binarySketch, expectedEstimate] = result;
      auto sketch =
          HLLSketch::deserialize(binarySketch.data(), binarySketch.size());
      u.update(sketch);
      double estimate = sketch.get_estimate();
      EXPECT_BETWEEN(
          expectedEstimate * 0.95, estimate, expectedEstimate * 1.05);
    }

    auto unionEstimate = u.get_result().get_estimate();
    EXPECT_BETWEEN(
        expectedUnionResult * 0.95, unionEstimate, expectedUnionResult * 1.05);
  }

  void verifyHLLSketch(
      const std::vector<std::string>& data,
      double expected,
      bool isStable = false) {
    VectorPtr varbinaryVector = toVarBinaryVector(data);

    auto plan = PlanBuilder()
                    .values({makeRowVector({varbinaryVector})})
                    .partialAggregation(
                        {},
                        {fmt::format(
                            "hll_sketch(c0, {})", isStable ? "true" : "false")})
                    .finalAggregation()
                    .planNode();

    auto result = readSingleValue(plan).value<double>();
    if (isStable) {
      int resultInt = static_cast<int>(result);
      int expectedInt = static_cast<int>(expected);

      EXPECT_EQ(resultInt, expectedInt);
    } else {
      EXPECT_BETWEEN(expected * 0.95, result, expected * 1.05);
    }
  }

  void verifyHLLSketchGroupBy(
      const std::vector<std::string>& data,
      int szKey,
      int expectedValue) {
    VectorPtr varbinaryVector = toVarBinaryVector(data);
    auto keys = makeFlatVector<int32_t>(
        data.size(), [&](auto row) { return row % szKey; });

    auto plan = PlanBuilder()
                    .values({makeRowVector({keys, varbinaryVector})})
                    .partialAggregation({"c0"}, {"hll_sketch(c1)"})
                    .finalAggregation()
                    .planNode();

    std::unordered_map<int32_t, double> expectedResultMin =
        toKeyValueMap(szKey, expectedValue * 0.95);
    std::unordered_map<int32_t, double> expectedResultMax =
        toKeyValueMap(szKey, expectedValue * 1.05);
    assertQueryBetween(
        plan, toRowVector(expectedResultMin), toRowVector(expectedResultMax));
  }
};

TEST_F(SketchHLLSketchTests, basic) {
  auto data1 = getBinarySketch<int32_t>(11, datasketches::HLL_4, 17);
  auto data2 = getBinarySketch<int32_t>(11, datasketches::HLL_4, 25, 12);

  // %17 or %25 will give 17, and 25 different elements
  // due to overlap, there will be 37 elements in their union
  verifySketchUnion({{data1, 17}, {data2, 25}}, 11, 37);

  // verify the same as above through hll_sketch function
  verifyHLLSketch({data1, data2}, 37);
}

TEST_F(SketchHLLSketchTests, different_lgK) {
  auto data1 = getBinarySketch<int32_t>(10, datasketches::HLL_4, 17);
  auto data2 = getBinarySketch<int32_t>(13, datasketches::HLL_4, 25, 12);

  // %17 or %25 will give 17, and 25 different elements
  // due to overlap, there will be 37 elements in their union
  verifySketchUnion({{data1, 17}, {data2, 25}}, 10, 37);

  // verify the same as above through hll_sketch function
  verifyHLLSketch({data1, data2}, 37);
}

TEST_F(SketchHLLSketchTests, different_type) {
  auto data1 = getBinarySketch<int32_t>(10, datasketches::HLL_4, 17);
  auto data2 = getBinarySketch<double>(13, datasketches::HLL_4, 25, 12);

  // %17 or %25 will give 17, and 25 different elements
  // though they to overlap, due different types, there will be 42 elements in
  // their union
  verifySketchUnion({{data1, 17}, {data2, 25}}, 10, 42);

  verifyHLLSketch({data1, data2}, 42);
}

TEST_F(SketchHLLSketchTests, groupBy) {
  auto binarySketches =
      getBinarySketches<int32_t>(12, datasketches::HLL_6, 2, 18);

  std::vector<std::pair<std::string, double>> results;
  std::transform(
      binarySketches.begin(),
      binarySketches.end(),
      std::back_inserter(results),
      [](std::string& str) { return std::make_pair(str, 9); });
  verifySketchUnion(results, 11, 18);

  verifyHLLSketch(binarySketches, 18);
}

TEST_F(SketchHLLSketchTests, stableEstimate) {
  auto data1 = getBinarySketch<int32_t>(11, datasketches::HLL_4, 17);
  verifyHLLSketch({data1}, 17, true);

  auto data2 = getBinarySketch<int32_t>(11, datasketches::HLL_4, 25, 12);
  verifyHLLSketch({data1, data2}, 37, true);
}

TEST_F(SketchHLLSketchTests, groupByGroupby) {
  auto binarySketches =
      getBinarySketches<int32_t>(12, datasketches::HLL_8, 6, 18);

  std::vector<std::pair<std::string, double>> results;
  std::transform(
      binarySketches.begin(),
      binarySketches.end(),
      std::back_inserter(results),
      [](std::string& str) { return std::make_pair(str, 3); });
  verifySketchUnion(results, 12, 18);

  verifyHLLSketchGroupBy(binarySketches, 3, 6);
}
} // namespace bytedance::bolt::aggregate::sketches::test
