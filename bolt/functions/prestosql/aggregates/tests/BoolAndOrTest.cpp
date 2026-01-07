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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
using namespace bytedance::bolt::functions::aggregate::test;
using bytedance::bolt::exec::test::PlanBuilder;
namespace bytedance::bolt::aggregate::test {

namespace {

struct TestParams {
  std::string boltName;
  std::string duckDbName;

  std::string toString() const {
    return boltName;
  }
};

class BoolAndOrTest : public virtual AggregationTestBase,
                      public testing::WithParamInterface<TestParams> {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }
};

TEST_P(BoolAndOrTest, basic) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), BOOLEAN()});
  auto vectors = makeVectors(rowType, 1000, 10);
  createDuckDbTable(vectors);

  const auto boltName = GetParam().boltName;
  const auto duckDbName = GetParam().duckDbName;

  const auto partialAgg = fmt::format("{}(c1)", boltName);

  // Global aggregation.
  testAggregations(
      vectors,
      {},
      {partialAgg},
      fmt::format("SELECT {}(c1::TINYINT) FROM tmp", duckDbName));

  // Group by aggregation.
  testAggregations(
      [&](auto& builder) {
        builder.values(vectors).project({"c0 % 10", "c1"});
      },
      {"p0"},
      {partialAgg},
      fmt::format(
          "SELECT c0 % 10, {}(c1::TINYINT) FROM tmp GROUP BY 1", duckDbName));

  // Encodings: use filter to wrap aggregation inputs in a dictionary.
  testAggregations(
      [&](auto& builder) {
        builder.values(vectors).filter("c0 % 2 = 0").project({"c0 % 11", "c1"});
      },
      {"p0"},
      {partialAgg},
      fmt::format(
          "SELECT c0 % 11, {}(c1::TINYINT) FROM tmp WHERE c0 % 2 = 0 GROUP BY 1",
          duckDbName));

  testAggregations(
      [&](auto& builder) { builder.values(vectors).filter("c0 % 2 = 0"); },
      {},
      {partialAgg},
      fmt::format(
          "SELECT {}(c1::TINYINT) FROM tmp WHERE c0 % 2 = 0", duckDbName));
}

BOLT_INSTANTIATE_TEST_SUITE_P(
    BoolAndOrTest,
    BoolAndOrTest,
    testing::Values(
        TestParams{"bool_and", "bit_and"},
        TestParams{"every", "bit_and"},
        TestParams{"bool_or", "bit_or"}),
    [](auto p) { return p.param.toString(); });

} // namespace
} // namespace bytedance::bolt::aggregate::test
