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

#include <folly/init/Init.h>

#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/parse/Expressions.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;

class ConjunctTest : public OperatorTestBase {};

TEST_F(ConjunctTest, mixed) {
  auto mixed = makeRowVector(
      {makeFlatVector<int32_t>(18, [](auto row) { return row; }),
       makeFlatVector<bool>(
           18,
           [](auto row) { return row % 3 == 0; },
           [](auto row) { return row % 3 == 2; }),
       makeFlatVector<bool>(
           18,
           [](auto row) { return (row / 3) % 3 == 0; },
           [](auto row) { return (row / 3) % 3 == 2; })});
  auto plan =
      PlanBuilder()
          .values({mixed})
          .project({"c0", "c1", "c2", "if (c0 <9, c1 and c2, c1 or c2)"})
          .planNode();
  createDuckDbTable({mixed});

  assertQuery(
      plan, "select c0, c1, c2, if (c0 < 9, c1 and c2, c1 or c2) from tmp");

  // Non-null and nullable
  auto mixedOneNull = makeRowVector(
      {makeFlatVector<int32_t>(18, [](auto row) { return row; }),
       makeFlatVector<bool>(18, [](auto row) { return row % 3 == 0; }),
       makeFlatVector<bool>(
           18,
           [](auto row) { return (row / 3) % 3 == 0; },
           [](auto row) { return (row / 3) % 3 == 2; })});
  plan = PlanBuilder()
             .values({mixedOneNull})
             .project({"c0", "c1", "c2", "if (c0 <9, c1 and c2, c1 or c2)"})
             .planNode();
  createDuckDbTable({mixedOneNull});

  assertQuery(
      plan, "select c0, c1, c2, if (c0 < 9, c1 and c2, c1 or c2) from tmp");

  // Both are non-null
  auto mixedNonNull = makeRowVector(
      {makeFlatVector<int32_t>(18, [](auto row) { return row; }),
       makeFlatVector<bool>(18, [](auto row) { return row % 3 == 0; }),
       makeFlatVector<bool>(18, [](auto row) { return (row / 3) % 3 == 0; })});
  plan = PlanBuilder()
             .values({mixedNonNull})
             .project({"c0", "c1", "c2", "if (c0 <9, c1 and c2, c1 or c2)"})
             .planNode();
  createDuckDbTable({mixedNonNull});

  assertQuery(
      plan, "select c0, c1, c2, if (c0 < 9, c1 and c2, c1 or c2) from tmp");
}

TEST_F(ConjunctTest, constant) {
  for (auto counter = 0; counter < 9; ++counter) {
    auto allSame = makeRowVector(
        {makeFlatVector<int32_t>(18, [&](auto row) { return row; }),
         makeFlatVector<bool>(
             18,
             [&](auto row) { return counter % 3 == 0; },
             [&](auto row) { return counter % 3 == 2; }),
         makeFlatVector<bool>(
             18,
             [&](auto row) { return (counter / 3) % 3 == 0; },
             [&](auto row) { return (counter / 3) % 3 == 2; })});
    auto plan =
        PlanBuilder()
            .values({allSame})
            .project({"c0", "c1", "c2", "if (c0 <9, c1 and c2, c1 or c2)"})
            .planNode();
    createDuckDbTable({allSame});

    assertQuery(
        plan, "select c0, c1, c2, if (c0 < 9, c1 and c2, c1 or c2) from tmp");
  }
}
