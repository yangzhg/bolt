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
#include "bolt/functions/lib/window/tests/WindowTestBase.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"
using namespace bytedance::bolt::exec::test;
namespace bytedance::bolt::window::test {

namespace {

static const std::vector<std::string> kRankFunctions = {
    std::string("rank()"),
    std::string("dense_rank()"),
    std::string("percent_rank()"),
    std::string("cume_dist()"),
    std::string("row_number()")};

// The RankTestBase class is used to instantiate parameterized window
// function tests. The parameters are based on the function being tested
// and a specific over clause. The window function is tested for the over
// clause and all combinations of frame clauses. Doing so amortizes the
// input vector and DuckDB table construction once across all the frame clauses
// for a (function, over clause) combination.
struct RankTestParam {
  const std::string function;
  const std::string overClause;
};

class RankTestBase : public WindowTestBase {
 protected:
  explicit RankTestBase(const RankTestParam& testParam)
      : function_(testParam.function), overClause_(testParam.overClause) {}

  void testWindowFunction(const std::vector<RowVectorPtr>& vectors) {
    WindowTestBase::testWindowFunction(vectors, function_, {overClause_});
  }

  void SetUp() override {
    WindowTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
  }

  const std::string function_;
  const std::string overClause_;
};

std::vector<RankTestParam> getRankTestParams() {
  std::vector<RankTestParam> params;
  for (auto function : kRankFunctions) {
    for (auto overClause : kOverClauses) {
      params.push_back({function, overClause});
    }
  }
  return params;
}

class RankTest : public RankTestBase,
                 public testing::WithParamInterface<RankTestParam> {
 public:
  RankTest() : RankTestBase(GetParam()) {}
};

// Tests all functions with a dataset with uniform distribution of partitions.
TEST_P(RankTest, basic) {
  testWindowFunction({makeSimpleVector(40)});
}

// Tests all functions with a dataset with all rows in a single partition,
// but in 2 input vectors.
TEST_P(RankTest, singlePartition) {
  testWindowFunction(
      {makeSinglePartitionVector(40), makeSinglePartitionVector(50)});
}

// Tests all functions with a dataset in which all partitions have a single row.
TEST_P(RankTest, singleRowPartitions) {
  testWindowFunction({makeSingleRowPartitionsVector(40)});
}

// Tests all functions with a dataset with randomly generated data.
TEST_P(RankTest, randomInput) {
  testWindowFunction({makeRandomInputVector(30)});
}

// Run above tests for all combinations of rank function and over clauses.
BOLT_INSTANTIATE_TEST_SUITE_P(
    RankTestInstantiation,
    RankTest,
    testing::ValuesIn(getRankTestParams()));

}; // namespace
}; // namespace bytedance::bolt::window::test
