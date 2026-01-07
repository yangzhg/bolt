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

#include <algorithm>

#include "bolt/common/base/RandomUtil.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::functions::aggregate::test;
namespace bytedance::bolt::aggregate::test {

namespace {

// Return the argument types of an aggregation when the aggregation is
// constructed by `functionCall` with the given dataType, accuracy,
// and percentileCount.
std::vector<TypePtr> getArgTypes(const TypePtr& dataType, int percentileCount) {
  std::vector<TypePtr> argTypes;
  argTypes.push_back(dataType);
  if (percentileCount == -1) {
    argTypes.push_back(DOUBLE());
  } else {
    argTypes.push_back(ARRAY(DOUBLE()));
  }
  return argTypes;
}

std::string functionCall(bool keyed, double percentile, int percentileCount) {
  std::ostringstream buf;
  int columnIndex = keyed;
  buf << "percentile(c" << columnIndex++;
  buf << ", ";
  if (percentileCount == -1) {
    buf << percentile;
  } else {
    buf << "ARRAY[";
    for (int i = 0; i < percentileCount; ++i) {
      buf << (i == 0 ? "" : ",") << percentile;
    }
    buf << ']';
  }
  buf << ')';
  return buf.str();
}

class HiveUDAFPercentileTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    random::setSeed(0);
    allowInputShuffle();
    AggregationTestBase::disableTestIncremental();
  }

  template <typename T>
  void testGlobalAgg(
      const VectorPtr& values,
      double percentile,
      std::optional<T> expectedResult) {
    SCOPED_TRACE(fmt::format("percentile={}", percentile));
    auto rows = makeRowVector({values});

    auto expected = expectedResult.has_value()
        ? fmt::format("SELECT {}", expectedResult.value())
        : "SELECT NULL";
    auto expectedArray = expectedResult.has_value()
        ? fmt::format("SELECT ARRAY[{0},{0},{0}]", expectedResult.value())
        : "SELECT NULL";
    enableTestStreaming();
    testAggregations(
        {rows}, {}, {functionCall(false, percentile, -1)}, expected);
    testAggregations(
        {rows}, {}, {functionCall(false, percentile, 3)}, expectedArray);

    // Companion functions of percentile do not support test streaming
    // because intermediate results are KLL that has non-deterministic shape.
    disableTestStreaming();
    testAggregationsWithCompanion(
        {rows},
        [](auto& /*builder*/) {},
        {},
        {functionCall(false, percentile, -1)},
        {getArgTypes(values->type(), -1)},
        {},
        expected);
    testAggregationsWithCompanion(
        {rows},
        [](auto& /*builder*/) {},
        {},
        {functionCall(false, percentile, 3)},
        {getArgTypes(values->type(), 3)},
        {},
        expectedArray);
  }

  void testGroupByAgg(
      const VectorPtr& keys,
      const VectorPtr& values,
      double percentile,
      const RowVectorPtr& expectedResult) {
    auto rows = makeRowVector({keys, values});
    enableTestStreaming();
    testAggregations(
        {rows}, {"c0"}, {functionCall(true, percentile, -1)}, {expectedResult});

    // Companion functions of percentile do not support test streaming
    // because intermediate results are KLL that has non-deterministic shape.
    disableTestStreaming();
    testAggregationsWithCompanion(
        {rows},
        [](auto& /*builder*/) {},
        {"c0"},
        {functionCall(true, percentile, -1)},
        {getArgTypes(values->type(), -1)},
        {},
        {expectedResult});

    {
      SCOPED_TRACE("Percentile array");
      auto resultValues = expectedResult->childAt(1);
      RowVectorPtr expected = nullptr;
      auto size = resultValues->size();
      if (resultValues->nulls() &&
          bits::countNonNulls(resultValues->rawNulls(), 0, size) == 0) {
        expected = makeRowVector(
            {expectedResult->childAt(0),
             BaseVector::createNullConstant(
                 ARRAY(resultValues->type()), size, pool())});
      } else {
        auto elements = BaseVector::create(
            resultValues->type(), 3 * resultValues->size(), pool());
        auto offsets = allocateOffsets(resultValues->size(), pool());
        auto rawOffsets = offsets->asMutable<vector_size_t>();
        auto sizes = allocateSizes(resultValues->size(), pool());
        auto rawSizes = sizes->asMutable<vector_size_t>();
        for (int i = 0; i < resultValues->size(); ++i) {
          rawOffsets[i] = 3 * i;
          rawSizes[i] = 3;
          elements->copy(resultValues.get(), 3 * i + 0, i, 1);
          elements->copy(resultValues.get(), 3 * i + 1, i, 1);
          elements->copy(resultValues.get(), 3 * i + 2, i, 1);
        }
        expected = makeRowVector(
            {expectedResult->childAt(0),
             std::make_shared<ArrayVector>(
                 pool(),
                 ARRAY(elements->type()),
                 nullptr,
                 resultValues->size(),
                 offsets,
                 sizes,
                 elements)});
      }

      enableTestStreaming();
      testAggregations(
          {rows}, {"c0"}, {functionCall(true, percentile, 3)}, {expected});

      // Companion functions of percentile do not support test streaming
      // because intermediate results are KLL that has non-deterministic shape.
      disableTestStreaming();
      testAggregationsWithCompanion(
          {rows},
          [](auto& /*builder*/) {},
          {"c0"},
          {functionCall(true, percentile, 3)},
          {getArgTypes(values->type(), 3)},
          {},
          {expected});
    }
  }
};

TEST_F(HiveUDAFPercentileTest, globalAgg) {
  vector_size_t size = 2;
  auto simpleValues =
      makeFlatVector<int32_t>(size, [](auto row) { return row * 10; });
  testGlobalAgg<double>(simpleValues, 0.3, 3.0f);

  size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row + 1; });

  testGlobalAgg<double>(values, 0.25, 250.75);

  auto reverseValues =
      makeFlatVector<int32_t>(size, [](auto row) { return 1000 - row; });

  testGlobalAgg<double>(reverseValues, 0.25, 250.75);

  size = 1'250;
  auto valuesWithNulls = makeFlatVector<int32_t>(
      size, [](auto row) { return row / 5 * 4 + row % 5; }, nullEvery(5));

  testGlobalAgg<double>(valuesWithNulls, 0.25, 250.75);
}

TEST_F(HiveUDAFPercentileTest, groupByAgg) {
  vector_size_t size = 5'000;
  auto keys =
      makeFlatVector<int64_t>(size, [](auto row) { return row / 1000; });
  auto values =
      makeFlatVector<int64_t>(size, [](auto row) { return row % 1000 + 1; });

  auto expectedResult = makeRowVector(
      {makeFlatVector(std::vector<int64_t>{0, 1, 2, 3, 4}),
       makeFlatVector(
           std::vector<double>{250.75, 250.75, 250.75, 250.75, 250.75})});
  testGroupByAgg(keys, values, 0.25, expectedResult);

  auto keysWithNulls =
      makeFlatVector<int64_t>(size, [](auto row) { return row / 1000; });
  auto valuesWithNulls = makeFlatVector<int64_t>(
      size, [](auto row) { return row % 1000 + 1; }, nullEvery(5));

  auto expectedResultWithNulls = makeRowVector(
      {makeFlatVector(std::vector<int64_t>{0, 1, 2, 3, 4}),
       makeFlatVector(std::vector<double>{251.5, 251.5, 251.5, 251.5, 251.5})});
  testGroupByAgg(keysWithNulls, valuesWithNulls, 0.25, expectedResultWithNulls);
}

TEST_F(HiveUDAFPercentileTest, partialFull) {
  // Make sure partial aggregation runs out of memory after first batch.
  CursorParameters params;
  params.queryCtx = core::QueryCtx::create(executor_.get());
  params.queryCtx->testingOverrideConfigUnsafe({
      {core::QueryConfig::kMaxPartialAggregationMemory, "300000"},
  });

  auto data = {
      makeRowVector({
          makeFlatVector<int32_t>(1'024, [](auto row) { return row % 128; }),
          makeFlatVector<int32_t>(1'024, [](auto /*row*/) { return 10; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(1'024, [](auto row) { return row % 128; }),
          makeFlatVector<int32_t>(1'024, [](auto /*row*/) { return 15; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(1'024, [](auto row) { return row % 128; }),
          makeFlatVector<int32_t>(1'024, [](auto /*row*/) { return 20; }),
      }),
  };

  params.planNode = PlanBuilder()
                        .values(data)
                        .project({"c0", "c1", "0.25", "1"})
                        .partialAggregation({"c0"}, {"percentile(c1, p2)"})
                        .finalAggregation()
                        .planNode();

  auto expected = makeRowVector({
      makeFlatVector<int32_t>(128, [](auto row) { return row; }),
      makeFlatVector<double>(128, [](auto row) { return 10; }),
  });
  exec::test::assertQuery(params, {expected});
}

TEST_F(HiveUDAFPercentileTest, noInput) {
  const int size = 1000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 7; });
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row % 6; });
  auto nullValues = makeNullConstant(TypeKind::INTEGER, size);

  // Test global.
  { testGlobalAgg<int32_t>(nullValues, 0.5, std::nullopt); }

  // Test group-by.
  {
    auto expected = makeRowVector(
        {makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6}),
         makeNullConstant(TypeKind::DOUBLE, 7)});

    testGroupByAgg(keys, nullValues, 0.5, expected);
  }

  // Test when all inputs are masked out.
  {
    auto testWithMask = [&](bool groupBy, const RowVectorPtr& expected) {
      std::vector<std::string> groupingKeys;
      if (groupBy) {
        groupingKeys.push_back("c0");
      }
      auto plan =
          PlanBuilder()
              .values({makeRowVector({keys, values})})
              .project(
                  {"c0", "c1", "array_constructor(0.5) as pct", "c1 > 6 as m1"})
              .singleAggregation(
                  groupingKeys,
                  {"percentile(c1, 0.5)",
                   "percentile(c1, 0.5)",
                   "percentile(c1, pct)",
                   "percentile(c1, pct)"},
                  {"m1", "m1", "m1", "m1"})
              .planNode();

      AssertQueryBuilder(plan).assertResults(expected);
    };

    // Global.
    std::vector<VectorPtr> children{4};
    std::fill_n(children.begin(), 2, makeNullConstant(TypeKind::DOUBLE, 1));
    std::fill_n(
        children.begin() + 2,
        2,
        BaseVector::createNullConstant(ARRAY(DOUBLE()), 1, pool()));
    auto expected1 = makeRowVector(children);
    testWithMask(false, expected1);

    // Group-by.
    children.resize(5);
    children[0] = makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6});
    std::fill_n(children.begin() + 1, 2, makeNullConstant(TypeKind::DOUBLE, 7));
    std::fill_n(
        children.begin() + 3,
        2,
        BaseVector::createNullConstant(ARRAY(DOUBLE()), 7, pool()));
    auto expected2 = makeRowVector(children);
    testWithMask(true, expected2);
  }
}

TEST_F(HiveUDAFPercentileTest, nullPercentile) {
  auto values = makeFlatVector<int32_t>({1, 2, 3, 4});
  auto percentileOfDouble = makeConstant<double>(std::nullopt, 4);
  auto rows = makeRowVector({values, percentileOfDouble});

  // Test null percentile for percentile(value, percentile).
  BOLT_ASSERT_THROW(
      testAggregations({rows}, {}, {"percentile(c0, c1)"}, "SELECT NULL"),
      "Percentile cannot be null");

  auto percentileOfArrayOfDouble = BaseVector::wrapInConstant(
      4,
      0,
      makeNullableArrayVector<double>(
          std::vector<std::vector<std::optional<double>>>{
              {std::nullopt, std::nullopt, std::nullopt, std::nullopt}}));
  rows = makeRowVector({values, percentileOfArrayOfDouble});

  // Test null percentile for percentile(value, percentiles).
  BOLT_ASSERT_THROW(
      testAggregations({rows}, {}, {"percentile(c0, c1)"}, "SELECT NULL"),
      "Percentile cannot be null");
}

} // namespace
} // namespace bytedance::bolt::aggregate::test
