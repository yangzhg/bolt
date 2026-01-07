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

#include "bolt/functions/lib/aggregates/BitDaysOrAggregate.h"
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
namespace bytedance::bolt::functions::aggregate::test {

class BitDaysOrAggTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerBitDaysOrAggregate("bit_days_or", true, false);
  }

  void testGroupByAgg(
      const VectorPtr& keys,
      const VectorPtr& values,
      const RowVectorPtr& expectedResult) {
    auto rows = makeRowVector({keys, values});
    createDuckDbTable({rows});
    testAggregations({rows}, {"c0"}, {"bit_days_or(c1)"}, {expectedResult});
  }

  void testGlobalAgg(
      const VectorPtr& values,
      const RowVectorPtr& expectedResult) {
    auto rows = makeRowVector({values});
    createDuckDbTable({rows});
    testAggregations({rows}, {}, {"bit_days_or(c0)"}, {expectedResult});
  }
};

TEST_F(BitDaysOrAggTest, groupBy) {
  {
    auto keys = makeFlatVector<int64_t>({1, 2, 1, 2});
    auto values =
        makeArrayVector<int64_t>({{7, 8, 9}, {4, 5, 6}, {6, 7, 8}, {7, 8, 9}});
    auto aggKeys = makeFlatVector<int64_t>({1, 2});
    auto aggResults = makeArrayVector<int64_t>({{7, 11, 9}, {7, 9, 11}});
    auto expectedResult =
        makeRowVector({"keys", "values"}, {aggKeys, aggResults});
    testGroupByAgg(keys, values, expectedResult);
  }

  {
    auto keys = makeFlatVector<int64_t>({1, 2, 1, 2});
    auto values = makeArrayVector<int64_t>(
        {{73479242, 84764793, 44270444},
         {47624720, 42692647, 26946294},
         {24692444, 20427027, 17492489},
         {23675021, 74297423, 39942444}});
    auto aggKeys = makeFlatVector<int64_t>({1, 2});
    auto aggResults = makeArrayVector<int64_t>(
        {{75101918, 88078715, 44821357}, {50328221, 82833519, 50035710}});
    auto expectedResult =
        makeRowVector({"keys", "values"}, {aggKeys, aggResults});
    testGroupByAgg(keys, values, expectedResult);
  }
}

TEST_F(BitDaysOrAggTest, globalAgg) {
  auto values = makeArrayVector<int64_t>(
      {{73479242, 84764793, 44270444},
       {47624720, 42692647, 26946294},
       {24692444, 20427027, 17492489},
       {23675021, 74297423, 39942444}});
  auto aggResults = makeArrayVector<int64_t>({{83883743, 100661631, 50068479}});
  auto expectedResult = makeRowVector({"values"}, {aggResults});
  testGlobalAgg(values, expectedResult);
}

} // namespace bytedance::bolt::functions::aggregate::test
