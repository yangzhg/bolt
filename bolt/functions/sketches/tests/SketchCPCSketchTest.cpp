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

#include <string>
#include "SketchTestBase.hpp"

#include "bolt/duckdb/conversion/DuckConversion.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/QueryAssertions.h"
#include "bolt/vector/VectorTypeUtils.h"
namespace bytedance::bolt::aggregate::sketches::test {

using CPCSketch = datasketches::cpc_sketch;

class SketchCPCSketchTests : public SketchTestBase {
 public:
  template <typename T>
  std::string getBinarySketch(int lgK, int mod, int low = 0) {
    vector_size_t size = 10'000;
    auto values =
        makeFlatVector<T>(size, [&](auto row) { return row % mod + low; });

    auto op = PlanBuilder()
                  .values({makeRowVector({values})})
                  .partialAggregation({}, {fmt::format("cpc(c0, {})", lgK)})
                  .planNode();

    return readSingleValue(op).template value<Varbinary>();
  }

  template <typename T>
  std::vector<std::string> getBinarySketches(int lgK, int szKey, int szVal) {
    vector_size_t size = 10'000;
    auto keys =
        makeFlatVector<int32_t>(size, [&](auto row) { return row % szKey; });
    auto values =
        makeFlatVector<T>(size, [&](auto row) { return row % szVal; });

    auto plan =
        PlanBuilder()
            .values({makeRowVector({keys, values})})
            .partialAggregation({"c0"}, {fmt::format("cpc(c1, {})", lgK)})
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
      double expectedUnionResult) {
    datasketches::cpc_union u;

    for (const auto& result : results) {
      auto [binarySketch, expectedEstimate] = result;
      auto sketch =
          CPCSketch::deserialize(binarySketch.data(), binarySketch.size());
      u.update(sketch);
      double estimate = sketch.get_estimate();
      EXPECT_BETWEEN(
          expectedEstimate * 0.95, estimate, expectedEstimate * 1.05);
    }

    auto unionEstimate = u.get_result().get_estimate();
    EXPECT_BETWEEN(
        expectedUnionResult * 0.95, unionEstimate, expectedUnionResult * 1.05);
  }

  void verifyCPCSketch(const std::vector<std::string>& data, double expected) {
    VectorPtr varbinaryVector = toVarBinaryVector(data);

    auto plan = PlanBuilder()
                    .values({makeRowVector({varbinaryVector})})
                    .partialAggregation({}, {"cpc_sketch(c0)"})
                    .finalAggregation()
                    .planNode();

    auto result = readSingleValue(plan).value<double>();
    EXPECT_BETWEEN(expected * 0.95, result, expected * 1.05);
  }

  void verifyCPCSketchGroupBy(
      const std::vector<std::string>& data,
      int szKey,
      int expectedValue) {
    VectorPtr varbinaryVector = toVarBinaryVector(data);
    auto keys = makeFlatVector<int32_t>(
        data.size(), [&](auto row) { return row % szKey; });

    auto plan = PlanBuilder()
                    .values({makeRowVector({keys, varbinaryVector})})
                    .partialAggregation({"c0"}, {"cpc_sketch(c1)"})
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

TEST_F(SketchCPCSketchTests, basic) {
  auto data1 = getBinarySketch<int32_t>(11, 17);
  auto data2 = getBinarySketch<int32_t>(11, 25, 12);

  // %17 or %25 will give 17, and 25 different elements
  // due to overlap, there will be 37 elements in their union
  verifySketchUnion({{data1, 17}, {data2, 25}}, 37);

  // verify the same as above through cpc_sketch function
  verifyCPCSketch({data1, data2}, 37);
}

TEST_F(SketchCPCSketchTests, different_lgK) {
  auto data1 = getBinarySketch<int32_t>(10, 17);
  auto data2 = getBinarySketch<int32_t>(13, 25, 12);

  // %17 or %25 will give 17, and 25 different elements
  // due to overlap, there will be 37 elements in their union
  verifySketchUnion({{data1, 17}, {data2, 25}}, 37);

  // verify the same as above through cpc_sketch function
  verifyCPCSketch({data1, data2}, 37);
}

TEST_F(SketchCPCSketchTests, different_type) {
  auto data1 = getBinarySketch<int32_t>(10, 17);
  auto data2 = getBinarySketch<double>(13, 25, 12);

  // %17 or %25 will give 17, and 25 different elements
  // though they to overlap, due different types, there will be 42 elements in
  // their union
  verifySketchUnion({{data1, 17}, {data2, 25}}, 42);
}

TEST_F(SketchCPCSketchTests, groupBy) {
  auto binarySketches = getBinarySketches<int32_t>(12, 2, 18);

  std::vector<std::pair<std::string, double>> results;
  std::transform(
      binarySketches.begin(),
      binarySketches.end(),
      std::back_inserter(results),
      [](std::string& str) { return std::make_pair(str, 9); });
  verifySketchUnion(results, 18);

  verifyCPCSketch(binarySketches, 18);
}

TEST_F(SketchCPCSketchTests, groupByGroupby) {
  auto binarySketches = getBinarySketches<int32_t>(12, 6, 18);

  std::vector<std::pair<std::string, double>> results;
  std::transform(
      binarySketches.begin(),
      binarySketches.end(),
      std::back_inserter(results),
      [](std::string& str) { return std::make_pair(str, 3); });
  verifySketchUnion(results, 18);

  verifyCPCSketchGroupBy(binarySketches, 3, 6);
}
} // namespace bytedance::bolt::aggregate::sketches::test
