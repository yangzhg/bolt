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

#include "bolt/common/caching/CacheTTLController.h"

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/caching/AsyncDataCache.h"
#include "bolt/common/caching/SsdCache.h"
#include "bolt/common/memory/MmapAllocator.h"
#include "gtest/gtest.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::memory;
namespace bytedance::bolt::cache {

class CacheTTLControllerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    allocator_ = std::make_shared<MmapAllocator>(
        MmapAllocator::Options{.capacity = 1024L * 1024L});
    cache_ = AsyncDataCache::create(allocator_.get());
  }

  std::shared_ptr<MemoryAllocator> allocator_;
  std::shared_ptr<AsyncDataCache> cache_;
};

TEST_F(CacheTTLControllerTest, addOpenFileInfo) {
  CacheTTLController::create(*cache_);

  EXPECT_TRUE(CacheTTLController::getInstance()->addOpenFileInfo(123L));
  EXPECT_FALSE(CacheTTLController::getInstance()->addOpenFileInfo(123L));

  EXPECT_TRUE(CacheTTLController::getInstance()->addOpenFileInfo(456L));
}

TEST_F(CacheTTLControllerTest, getCacheAgeStats) {
  CacheTTLController::create(*cache_);

  int64_t fileOpenTime = getCurrentTimeSec();
  for (auto i = 0; i < 1000; i++) {
    CacheTTLController::getInstance()->addOpenFileInfo(i, fileOpenTime + i);
  }

  int64_t current = getCurrentTimeSec();
  EXPECT_GE(
      CacheTTLController::getInstance()->getCacheAgeStats().maxAgeSecs,
      current - fileOpenTime);
}
} // namespace bytedance::bolt::cache
