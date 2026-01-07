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

#include "bolt/common/base/ScopedLock.h"

#include <gtest/gtest.h>
namespace bytedance::bolt {

TEST(ScopedLockTest, basic) {
  int count = 0;
  std::mutex mu;
  {
    ScopedLock sl(&mu);
    sl.addCallback([&]() {
      std::lock_guard<std::mutex> l(mu);
      ++count;
    });
    ++count;
  }
  ASSERT_EQ(count, 2);
}

TEST(ScopedLockTest, multiCallbacks) {
  int count = 0;
  const int numCallbacks = 10;
  std::mutex mu;
  {
    ScopedLock sl(&mu);
    for (int i = 0; i < numCallbacks; ++i) {
      sl.addCallback([&]() {
        std::lock_guard<std::mutex> l(mu);
        ++count;
      });
    }
    ++count;
  }
  ASSERT_EQ(count, 1 + numCallbacks);
}

} // namespace bytedance::bolt
