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

#include "bolt/dwio/dwrf/reader/StreamLabels.h"

using namespace ::testing;
using bytedance::bolt::memory::AllocationPool;
using namespace bytedance::bolt::dwrf;

class StreamLabelsTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    bytedance::bolt::memory::MemoryManager::testingSetInstance(
        bytedance::bolt::memory::MemoryManager::Options{});
  }
};

TEST_F(StreamLabelsTest, E2E) {
  auto pool = bytedance::bolt::memory::memoryManager()->addLeafPool();
  AllocationPool allocationPool(pool.get());
  StreamLabels root(allocationPool);
  auto c0 = root.append("c0");
  auto c1 = root.append("c1");
  auto c0_f0 = c0.append("f0");
  auto c1_f0 = c1.append("f0");
  EXPECT_EQ(root.label(), "/");
  EXPECT_EQ(c0.label(), "/c0");
  EXPECT_EQ(c1.label(), "/c1");
  EXPECT_EQ(c0_f0.label(), "/c0/f0");
  EXPECT_EQ(c1_f0.label(), "/c1/f0");
}
