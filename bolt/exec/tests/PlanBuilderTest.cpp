/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/exec/WindowFunction.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
namespace bytedance::bolt::exec::test {

class PlanBuilderTest : public testing::Test,
                        public bolt::test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  PlanBuilderTest() {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();
  }
};

TEST_F(PlanBuilderTest, invalidSourceNode) {
  BOLT_ASSERT_THROW(
      PlanBuilder().project({"c0 > 5"}).planNode(),
      "Project cannot be the source node");
  BOLT_ASSERT_THROW(
      PlanBuilder().filter({"c0 > 5"}).planNode(),
      "Filter cannot be the source node");
}

TEST_F(PlanBuilderTest, duplicateSubfield) {
  BOLT_ASSERT_THROW(
      PlanBuilder(pool_.get())
          .tableScan(
              ROW({"a", "b"}, {BIGINT(), BIGINT()}),
              {"a < 5", "b = 7", "a > 0"},
              "a + b < 100")
          .planNode(),
      "Duplicate subfield: a");
}

TEST_F(PlanBuilderTest, invalidScalarFunctionCall) {
  BOLT_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b"}, {BIGINT(), BIGINT()}))
          .project({"to_unixtime(a)"})
          .planNode(),
      "Scalar function signature is not supported: to_unixtime(BIGINT).");

  BOLT_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b"}, {BIGINT(), BIGINT()}))
          .project({"to_unitime(a)"})
          .planNode(),
      "Scalar function doesn't exist: to_unitime.");
}

TEST_F(PlanBuilderTest, invalidAggregateFunctionCall) {
  BOLT_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b"}, {VARCHAR(), BIGINT()}))
          .partialAggregation({}, {"sum(a)"})
          .planNode(),
      "Aggregate function signature is not supported: sum(VARCHAR).");

  BOLT_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b"}, {VARCHAR(), BIGINT()}))
          .partialAggregation({}, {"maxx(a)"})
          .planNode(),
      "Aggregate function doesn't exist: maxx.");
}

namespace {

void registerWindowFunction() {
  std::vector<exec::FunctionSignaturePtr> signatures{
      exec::FunctionSignatureBuilder()
          .argumentType("BIGINT")
          .returnType("BIGINT")
          .build(),
  };
  exec::registerWindowFunction("window1", std::move(signatures), nullptr);
}
} // namespace

TEST_F(PlanBuilderTest, windowFunctionCall) {
  BOLT_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window({"window1(c) over (partition by a order by b) as d"})
          .planNode(),
      "Window function doesn't exist: window1.");

  registerWindowFunction();

  BOLT_CHECK_EQ(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window({"window1(c) over (partition by a order by b) as d"})
          .planNode()
          ->toString(true, false, true),
      "-- Window[1][partition by [a] order by [b ASC NULLS LAST] "
      "d := window1(ROW[\"c\"]) RANGE between UNBOUNDED PRECEDING and CURRENT ROW] "
      "-> a:VARCHAR, b:BIGINT, c:BIGINT, d:BIGINT\n");

  BOLT_CHECK_EQ(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window({"window1(c) over (partition by a) as d"})
          .planNode()
          ->toString(true, false, true),
      "-- Window[1][partition by [a] "
      "d := window1(ROW[\"c\"]) RANGE between UNBOUNDED PRECEDING and CURRENT ROW] "
      "-> a:VARCHAR, b:BIGINT, c:BIGINT, d:BIGINT\n");

  BOLT_CHECK_EQ(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window({"window1(c) over ()"})
          .planNode()
          ->toString(true, false, true),
      "-- Window[1][w0 := window1(ROW[\"c\"]) RANGE between UNBOUNDED PRECEDING and CURRENT ROW] "
      "-> a:VARCHAR, b:BIGINT, c:BIGINT, w0:BIGINT\n");

  BOLT_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b"}, {VARCHAR(), BIGINT()}))
          .window({"window1(a) over (partition by a order by b) as d"})
          .planNode(),
      "Window function signature is not supported: window1(VARCHAR).");

  BOLT_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b"}, {VARCHAR(), BIGINT()}))
          .window({"window2(a) over (partition by a order by b) as d"})
          .planNode(),
      "Window function doesn't exist: window2.");
}

TEST_F(PlanBuilderTest, windowFrame) {
  registerWindowFunction();

  // Validating that function invocations with different frames but the same
  // partitioning and order can be executed in the same node.
  BOLT_CHECK_EQ(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window(
              {"window1(c) over (partition by a order by b rows between b preceding and current row) as d1",
               "window1(c) over (partition by a order by b range between b preceding and current row) as d2",
               "window1(c) over (partition by a order by b rows between unbounded preceding and current row) as d3",
               "window1(c) over (partition by a order by b range between unbounded preceding and current row) as d4",
               "window1(c) over (partition by a order by b rows between current row and b following) as d5",
               "window1(c) over (partition by a order by b range between current row and b following) as d6",
               "window1(c) over (partition by a order by b rows between current row and unbounded following) as d7",
               "window1(c) over (partition by a order by b range between current row and unbounded following) as d8",
               "window1(c) over (partition by a order by b rows between unbounded preceding and unbounded following) as d9",
               "window1(c) over (partition by a order by b range between unbounded preceding and unbounded following) as d10"})
          .planNode()
          ->toString(true, false, true),
      "-- Window[1][partition by [a] order by [b ASC NULLS LAST] "
      "d1 := window1(ROW[\"c\"]) ROWS between b PRECEDING and CURRENT ROW, "
      "d2 := window1(ROW[\"c\"]) RANGE between b PRECEDING and CURRENT ROW, "
      "d3 := window1(ROW[\"c\"]) ROWS between UNBOUNDED PRECEDING and CURRENT ROW, "
      "d4 := window1(ROW[\"c\"]) RANGE between UNBOUNDED PRECEDING and CURRENT ROW, "
      "d5 := window1(ROW[\"c\"]) ROWS between CURRENT ROW and b FOLLOWING, "
      "d6 := window1(ROW[\"c\"]) RANGE between CURRENT ROW and b FOLLOWING, "
      "d7 := window1(ROW[\"c\"]) ROWS between CURRENT ROW and UNBOUNDED FOLLOWING, "
      "d8 := window1(ROW[\"c\"]) RANGE between CURRENT ROW and UNBOUNDED FOLLOWING, "
      "d9 := window1(ROW[\"c\"]) RANGE between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING, "
      "d10 := window1(ROW[\"c\"]) RANGE between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING] "
      "-> a:VARCHAR, b:BIGINT, c:BIGINT, d1:BIGINT, d2:BIGINT, d3:BIGINT, d4:BIGINT, "
      "d5:BIGINT, d6:BIGINT, d7:BIGINT, d8:BIGINT, d9:BIGINT, d10:BIGINT\n");

  BOLT_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window(
              {"window1(c) over (partition by a order by b rows between b preceding and current row) as d1",
               "window1(c) over (partition by a order by b range between b preceding and current row) as d2",
               "window1(c) over (partition by b order by a rows between b preceding and current row) as d3"})
          .planNode(),
      "do not match PARTITION BY clauses.");

  BOLT_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window(
              {"window1(c) over (partition by a order by b rows between b preceding and current row) as d1",
               "window1(c) over (partition by a order by c rows between b preceding and current row) as d2"})
          .planNode(),
      "do not match ORDER BY clauses.");

  BOLT_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW({"a", "b", "c"}, {VARCHAR(), BIGINT(), BIGINT()}))
          .window(
              {"window1(c) over (partition by a order by b rows between b preceding and current row) as d1",
               "window1(c) over (partition by a order by b desc rows between b preceding and current row) as d2"})
          .planNode(),
      "do not match ORDER BY clauses.");

  BOLT_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW(
              {"a", "b", "c", "d"}, {VARCHAR(), BIGINT(), BIGINT(), BIGINT()}))
          .window({
              "window1(c) over (partition by a order by b, c range between d preceding and current row) as d1",
          })
          .planNode(),
      "Window frame of type RANGE PRECEDING or FOLLOWING requires single sorting key in ORDER BY");

  BOLT_ASSERT_THROW(
      PlanBuilder()
          .tableScan(ROW(
              {"a", "b", "c", "d"}, {VARCHAR(), BIGINT(), BIGINT(), BIGINT()}))
          .window({
              "window1(c) over (partition by a, c range between d preceding and current row) as d1",
          })
          .planNode(),
      "Window frame of type RANGE PRECEDING or FOLLOWING requires single sorting key in ORDER BY");
}
} // namespace bytedance::bolt::exec::test
