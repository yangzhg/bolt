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

#include <gtest/gtest.h>

#include "bolt/exec/Aggregate.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
namespace bytedance::bolt::aggregate::test {

class AggregationFunctionRegTest : public testing::Test {
 protected:
  void clearAndCheckRegistry() {
    exec::aggregateFunctions().withWLock([&](auto& functionsMap) {
      functionsMap.clear();
      EXPECT_EQ(0, functionsMap.size());
    });
  }
};

TEST_F(AggregationFunctionRegTest, prefix) {
  // Remove all functions and check for no entries.
  clearAndCheckRegistry();

  // Register without prefix and memorize function maps.
  aggregate::prestosql::registerAllAggregateFunctions();
  const auto aggrFuncMapBase = exec::aggregateFunctions().copy();

  // Remove all functions and check for no entries.
  clearAndCheckRegistry();

  // Register with prefix and check all functions have the prefix.
  const std::string prefix{"test.abc_schema."};
  aggregate::prestosql::registerAllAggregateFunctions(prefix);
  exec::aggregateFunctions().withRLock([&](const auto& aggrFuncMap) {
    for (const auto& entry : aggrFuncMap) {
      EXPECT_EQ(prefix, entry.first.substr(0, prefix.size()));
      EXPECT_EQ(1, aggrFuncMapBase.count(entry.first.substr(prefix.size())));
    }
  });
}

} // namespace bytedance::bolt::aggregate::test
