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

#include <DataSketches/kll_sketch.hpp>

#include "bolt/duckdb/conversion/DuckConversion.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/QueryAssertions.h"
#include "bolt/functions/sketches/SerDe.hpp"
#include "bolt/functions/sketches/SketchAggregateBase.h"
#include "bolt/functions/sketches/tests/SketchTestBase.hpp"
#include "bolt/vector/VectorTypeUtils.h"
namespace bytedance::bolt::aggregate::sketches::test {

template <typename T>
using KLLSketch = datasketches::kll_sketch<T, std::less<T>, serde<T>>;

class SketchKLLSketchTests : public SketchTestBase {
 public:
  template <typename T>
  std::string getPartial(uint16_t k, double rank, int mod, int low = 0) {
    auto values =
        makeFlatVector<T>(mod, [&](auto row) { return row % mod + low; });

    auto op = PlanBuilder()
                  .values({makeRowVector({values})})
                  .partialAggregation(
                      {}, {fmt::format("kll(c0, {}, {:.{}f})", k, rank, 5)})
                  .planNode();

    return readSingleValue(op).template value<Varbinary>();
  }

  template <typename T>
  std::string getSerializedSketch(uint16_t k, int mod, int low = 0) {
    constexpr int headerSize = 1;
    KLLSketch<T> sketch(k);
    for (auto i = 0; i < mod; i++) {
      sketch.update((T)(i + low));
    }

    auto expected = low + mod / 2;
    EXPECT_BETWEEN(expected * 0.95, expected * 1.0, expected * 1.05);

    auto serialized = sketch.serialize(headerSize);
    serialized[0] = headerSize;
    return std::string(serialized.begin(), serialized.end());
  }

  template <typename T>
  std::vector<std::string>
  getBinarySketches(int k, double rank, int szKey, int szVal) {
    vector_size_t size = 10'000;
    auto keys =
        makeFlatVector<int32_t>(size, [&](auto row) { return row % szKey; });
    auto values =
        makeFlatVector<T>(size, [&](auto row) { return row % szVal; });

    auto aggregation = fmt::format("kll(c1, {}, {:.{}f})", k, rank, 5);

    auto plan = PlanBuilder()
                    .values({makeRowVector({keys, values})})
                    .partialAggregation({"c0"}, {aggregation})
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
  void verifySketchUnion(
      const std::vector<std::pair<std::string, double>>& results,
      double rank,
      double expectedUnionResult) {
    KLLSketch<T> u;
    for (const auto& result : results) {
      auto [str, expectedEstimate] = result;
      int headerSize = str[0];
      auto sketch = KLLSketch<T>::deserialize(
          str.data() + headerSize, str.size() - headerSize);
      u.merge(sketch);
      T estimate = sketch.get_quantile(rank);
      EXPECT_BETWEEN(
          expectedEstimate * 0.5, 1.0 * estimate, expectedEstimate * 2.0);
    }

    auto unionEstimate = u.get_quantile(rank);
    EXPECT_BETWEEN(
        expectedUnionResult * 0.5,
        1.0 * unionEstimate,
        expectedUnionResult * 2);
  }

  template <typename T>
  void verifyKLLSketch(
      const std::vector<std::string>& data,
      uint16_t k,
      double rank,
      T expected) {
    VectorPtr varbinaryVector = toVarBinaryVector(data);

    auto typeName = typeNameMap[typeid(T).name()];
    auto aggregate =
        fmt::format("kll_sketch_{}(c0, {}, {:.{}f})", typeName, k, rank, 5);
    auto plan = PlanBuilder()
                    .values({makeRowVector({varbinaryVector})})
                    .partialAggregation({}, {aggregate})
                    .finalAggregation()
                    .planNode();

    auto result = readSingleValue(plan).value<T>();
    EXPECT_BETWEEN(expected * 0.5, 1.0 * result, expected * 2.0);
  }

  template <typename T>
  void verifyKLLSketchGroupBy(
      const std::vector<std::string>& data,
      uint16_t k,
      double rank,
      int szKey) {
    VectorPtr varbinaryVector = toVarBinaryVector(data);
    auto keys = makeFlatVector<int32_t>(
        data.size(), [&](auto row) { return row % szKey; });

    auto typeName = typeNameMap[typeid(T).name()];
    auto aggregate =
        fmt::format("kll_sketch_{}(c1, {}, {:.{}f})", typeName, k, rank, 5);
    auto plan = PlanBuilder()
                    .values({makeRowVector({keys, varbinaryVector})})
                    .partialAggregation({"c0"}, {aggregate})
                    .finalAggregation()
                    .planNode();

    std::unordered_map<int32_t, double> expectedResultMin;
    std::unordered_map<int32_t, double> expectedResultMax;
    for (int i = 0; i < szKey; i++) {
      expectedResultMin[i] = 0;
      expectedResultMax[i] = DBL_MAX;
    }

    assertQueryBetween(
        plan, toRowVector(expectedResultMin), toRowVector(expectedResultMax));
  }

  template <typename T>
  void verifyBasic() {
    auto data1 = getSerializedSketch<T>(200, 17);
    auto data2 = getSerializedSketch<T>(200, 17, 15);

    // %17 or %17+15 will give median of 8, and 8+15
    // due to overlap of two, 16 will be the median
    verifySketchUnion<T>({{data1, 8}, {data2, 15 + 8}}, 0.5, 16);

    // verify the same as above through kll_sketch function
    // this verifies consumption of serialized sketches
    verifyKLLSketch<T>({data1, data2}, 200, 0.5, 16);
  }

  template <typename T>
  void verifyBasicPartial() {
    auto rank = 0.5;
    uint32_t k = 200;
    auto mod = 17;

    auto data1 = getPartial<T>(k, rank, mod);
    auto data2 = getPartial<T>(k, rank, mod, mod);

    // this verifies consumption of partial results
    verifyKLLSketch<T>({data1, data2}, k, rank, mod);
  }

  template <typename T>
  void verifyGroupBy() {
    auto binarySketches = getBinarySketches<T>(256, 0.5, 2, 18);
    std::vector<T> expecteds{8, 9};

    std::vector<std::pair<std::string, double>> results;
    for (int i = 0; i < binarySketches.size(); i++) {
      results.push_back(make_pair(binarySketches[i], expecteds[i]));
    }

    verifySketchUnion<T>(results, 0.5, 9);

    verifyKLLSketch<T>(binarySketches, 256, 0.5, 9);
  }
};

TEST_F(SketchKLLSketchTests, partialResults) {
  constexpr auto rankIdx = 1;
  auto rank = 0.5;
  uint32_t k = 200;
  auto mod = 17;

  auto partialResult = getPartial<double>(k, rank, mod);
  auto data = partialResult.data();
  int headerSize = data[0];
  EXPECT_EQ(rank, *(reinterpret_cast<const double*>(data + rankIdx)));

  auto sketch = KLLSketch<double>::deserialize(
      data + headerSize, partialResult.size() - headerSize);
  EXPECT_EQ(sketch.get_k(), k);
  EXPECT_EQ(sketch.get_quantile(rank), mod / 2);
}

TEST_F(SketchKLLSketchTests, basic_int8) {
  verifyBasicPartial<int8_t>();
}

TEST_F(SketchKLLSketchTests, basic_int16) {
  verifyBasicPartial<int16_t>();
}

TEST_F(SketchKLLSketchTests, basic_int32) {
  verifyBasicPartial<int32_t>();
}

TEST_F(SketchKLLSketchTests, basic_int64) {
  verifyBasicPartial<int64_t>();
}

TEST_F(SketchKLLSketchTests, basic_float) {
  verifyBasicPartial<float>();
}

TEST_F(SketchKLLSketchTests, double) {
  verifyBasicPartial<double>();
}

TEST_F(SketchKLLSketchTests, basicPartial_int8) {
  verifyBasic<int8_t>();
}

TEST_F(SketchKLLSketchTests, basicPartial_int16) {
  verifyBasic<int16_t>();
}

TEST_F(SketchKLLSketchTests, basicPartial_int32) {
  verifyBasic<int32_t>();
}

TEST_F(SketchKLLSketchTests, basicPartial_int64) {
  verifyBasic<int64_t>();
}

TEST_F(SketchKLLSketchTests, basicPartial_float) {
  verifyBasic<float>();
}

TEST_F(SketchKLLSketchTests, basicPartial_double) {
  verifyBasic<double>();
}

TEST_F(SketchKLLSketchTests, basicPartial_differentRank) {
  uint32_t k = 200;
  auto mod = 17;

  auto data1 = getPartial<double>(k, 0, mod);
  auto data2 = getPartial<double>(k, 1.0, mod, mod);

  // It will toss away rank part from data1, data2, and use new rank
  // which is 0.5 to get quantiles.
  verifyKLLSketch<double>({data1, data2}, k, 0.5, mod);
}

TEST_F(SketchKLLSketchTests, groupBy_int8) {
  verifyGroupBy<int8_t>();
}

TEST_F(SketchKLLSketchTests, groupBy_int16) {
  verifyGroupBy<int16_t>();
}

TEST_F(SketchKLLSketchTests, groupBy_int32) {
  verifyGroupBy<int32_t>();
}

TEST_F(SketchKLLSketchTests, groupBy_int64) {
  verifyGroupBy<int64_t>();
}

TEST_F(SketchKLLSketchTests, groupBy_float) {
  verifyGroupBy<float>();
}

TEST_F(SketchKLLSketchTests, groupBy_double) {
  verifyGroupBy<double>();
}

TEST_F(SketchKLLSketchTests, groupByGroupby) {
  auto binarySketches = getBinarySketches<double>(256, 0.5, 6, 18);
  std::vector<double> expecteds1{6, 7, 8, 9, 10, 11};

  std::vector<std::pair<std::string, double>> results;
  for (int i = 0; i < binarySketches.size(); i++) {
    results.push_back(make_pair(binarySketches[i], expecteds1[i]));
  }

  verifySketchUnion<double>(results, 0.5, 9);

  verifyKLLSketchGroupBy<double>(binarySketches, 512, 0.5, 3);
}
} // namespace bytedance::bolt::aggregate::sketches::test
