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

#include "bolt/exec/HashBitRange.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;

class HashRangeBitTest : public test::VectorTestBase, public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TEST_F(HashRangeBitTest, hashBitRange) {
  HashBitRange bitRange(29, 31);
  ASSERT_EQ(29, bitRange.begin());
  ASSERT_EQ(4, bitRange.numPartitions());
  ASSERT_EQ(2, bitRange.numBits());
  ASSERT_EQ(31, bitRange.end());
  ASSERT_EQ(bitRange, bitRange);

  HashBitRange defaultRange;
  ASSERT_EQ(0, defaultRange.begin());
  ASSERT_EQ(1, defaultRange.numPartitions());
  ASSERT_EQ(0, defaultRange.numBits());
  ASSERT_EQ(0, defaultRange.end());
  ASSERT_EQ(defaultRange, defaultRange);
  ASSERT_NE(defaultRange, bitRange);

  // Error test cases.
  HashBitRange validRange(63, 64);
  ASSERT_ANY_THROW(HashBitRange(63, 65));
  ASSERT_ANY_THROW(HashBitRange(65, 65));
}
