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

#include "bolt/exec/ProbeOperatorState.h"
#include <gtest/gtest.h>
namespace bytedance::bolt::exec::test {
class ProbeOperatorStateTest : public testing::Test {};

TEST_F(ProbeOperatorStateTest, basic) {
  ASSERT_EQ(
      probeOperatorStateName(ProbeOperatorState::kWaitForBuild),
      "WAIT_FOR_BUILD");
  ASSERT_EQ(probeOperatorStateName(ProbeOperatorState::kRunning), "RUNNING");
  ASSERT_EQ(
      probeOperatorStateName(ProbeOperatorState::kWaitForPeers),
      "WAIT_FOR_PEERS");
  ASSERT_EQ(probeOperatorStateName(ProbeOperatorState::kFinish), "FINISH");
  ASSERT_EQ(
      probeOperatorStateName(static_cast<ProbeOperatorState>(-1)),
      "UNKNOWN: -1");
}

} // namespace bytedance::bolt::exec::test
