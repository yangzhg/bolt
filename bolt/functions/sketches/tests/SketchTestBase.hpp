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

#pragma once

#include <fstream>
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/prestosql/tests//utils/FunctionBaseTest.h"
#include "bolt/functions/sketches/RegistrationAggregateFunctions.h"
#include "bolt/functions/sketches/RegistrationFunctions.h"
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
namespace bytedance::bolt::aggregate::sketches::test {

#define EXPECT_BETWEEN(x, y, z) \
  EXPECT_LE(x, y);              \
  EXPECT_LE(y, z);

using bytedance::bolt::functions::test::FunctionBaseTest;

struct SketchTestBase : public exec::test::OperatorTestBase {
  static const std::vector<std::string> kFruits;
  static const std::vector<std::string> kVegetables;
  void SetUp() override {
    OperatorTestBase::SetUp();
    bytedance::bolt::aggregate::sketches::registerAllAggregateFunctions();
    bytedance::bolt::functions::sketches::registerAllScalarFunctions();
  }
  template <typename T>
  void testGlobalAgg(
      const VectorPtr& values,
      const std::string& strAggr,
      T expectedMin,
      T expectedMax) {
    auto op = PlanBuilder()
                  .values({makeRowVector({values})})
                  .singleAggregation({}, {strAggr})
                  .planNode();
    auto val = readSingleValue(op).value<T>();
    EXPECT_TRUE((val >= expectedMin) && (val <= expectedMax));

    op = PlanBuilder()
             .values({makeRowVector({values})})
             .partialAggregation({}, {strAggr})
             .finalAggregation()
             .planNode();
    val = readSingleValue(op).value<T>();
    EXPECT_TRUE((val >= expectedMin) && (val <= expectedMax));
  }

  template <typename T, typename U>
  RowVectorPtr toRowVector(const std::unordered_map<T, U>& data) {
    std::vector<T> keys(data.size());
    transform(data.begin(), data.end(), keys.begin(), [](auto pair) {
      return pair.first;
    });

    std::vector<U> values(data.size());
    transform(data.begin(), data.end(), values.begin(), [](auto pair) {
      return pair.second;
    });

    return makeRowVector({makeFlatVector(keys), makeFlatVector(values)});
  }

  template <typename K, typename V>
  void testGroupByAgg(
      const VectorPtr& keys,
      const VectorPtr& values,
      const std::string& strAggr,
      const std::unordered_map<K, V>& expectedResultsMin,
      const std::unordered_map<K, V>& expectedResultsMax) {
    RowVectorPtr expectedMin = toRowVector(expectedResultsMin);
    RowVectorPtr expectedMax = toRowVector(expectedResultsMax);

    core::PlanNodePtr op = PlanBuilder()
                               .values({makeRowVector({keys, values})})
                               .singleAggregation({"c0"}, {strAggr})
                               .planNode();
    assertQueryBetween(op, expectedMin, expectedMax);

    op = PlanBuilder()
             .values({makeRowVector({keys, values})})
             .partialAggregation({"c0"}, {strAggr})
             .finalAggregation()
             .planNode();
    assertQueryBetween(op, expectedMin, expectedMax);
  }

  VectorPtr toVarBinaryVector(const std::vector<std::string>& data) {
    VectorPtr varbinaryVector =
        BaseVector::create(VARBINARY(), data.size(), pool());
    auto flatVector = varbinaryVector->asFlatVector<StringView>();
    for (int i = 0; i < data.size(); i++) {
      flatVector->set(i, StringView(data[i]));
    }
    return varbinaryVector;
  }

  std::unordered_map<int32_t, double> toKeyValueMap(int sz, double value) {
    std::unordered_map<int32_t, double> ret;
    for (int i = 0; i < sz; i++) {
      ret[i] = value;
    }
    return ret;
  }
};

} // namespace bytedance::bolt::aggregate::sketches::test
