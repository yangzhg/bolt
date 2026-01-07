/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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
 */

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cstdint>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/caching/AsyncDataCache.h"
#include "bolt/common/caching/SsdCache.h"
#include "bolt/common/memory/MallocAllocator.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/common/memory/MmapAllocator.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/exec/SharedArbitrator.h"

using namespace ::testing;
using namespace bytedance::bolt::cache;
using namespace bytedance::bolt::common::testutil;

constexpr int64_t KB = 1024L;
constexpr int64_t MB = 1024L * KB;
constexpr int64_t GB = 1024L * MB;
namespace bytedance {
namespace bolt {
namespace memory {

class MemoryAllocationListenerTest : public testing::Test {
 protected:
  static constexpr uint64_t kDefaultCapacity = 8 * GB; // 8GB
  static constexpr uint64_t kDefaultSingleAllocationThreshold = 8 * MB;
  static constexpr uint64_t kDefaultAccumulativeAllocationThreshold = 64 * MB;
};

TEST_F(MemoryAllocationListenerTest, BothMode) {
  const uint64_t kMaxCap = 128L * MB;
  MemoryManager::Options options = {
      .allocatorCapacity = kDefaultCapacity,
      .singleAllocationThreshold = kDefaultSingleAllocationThreshold,
      .accumulativeAllocationThreshold =
          kDefaultAccumulativeAllocationThreshold};
  std::shared_ptr<MemoryManager> manager =
      std::make_shared<MemoryManager>(options);
  auto root = manager->addRootPool("MemoryAllocationListenerTest", kMaxCap);
  auto pool = root->addLeafChild("static_quota", true);
  ASSERT_EQ(0, pool->currentBytes());
  // allocate
  void* a = pool->allocate(1 * MB);
  void* b = pool->allocate(9 * MB);
  void* c = pool->allocate(16 * MB);
  void* d = pool->allocate(16 * MB);
  void* e = pool->allocate(16 * MB);
  void* f = pool->allocate(7 * MB);
  // free
  pool->free(a, 1 * MB);
  pool->free(b, 9 * MB);
  pool->free(c, 16 * MB);
  pool->free(d, 16 * MB);
  pool->free(e, 16 * MB);
  pool->free(f, 7 * MB);
  ASSERT_EQ(0, pool->currentBytes());
}

TEST_F(MemoryAllocationListenerTest, AccumulativeMode) {
  const uint64_t kMaxCap = 128L * MB;
  MemoryManager::Options options = {
      .allocatorCapacity = kDefaultCapacity,
      .singleAllocationThreshold = 0,
      .accumulativeAllocationThreshold =
          kDefaultAccumulativeAllocationThreshold};
  std::shared_ptr<MemoryManager> manager =
      std::make_shared<MemoryManager>(options);
  auto root = manager->addRootPool("MemoryAllocationListenerTest", kMaxCap);
  auto pool = root->addLeafChild("static_quota", true);
  ASSERT_EQ(0, pool->currentBytes());
  // allocate
  void* a = pool->allocate(1 * MB);
  void* b = pool->allocate(9 * MB);
  void* c = pool->allocate(16 * MB);
  void* d = pool->allocate(16 * MB);
  void* e = pool->allocate(16 * MB);
  void* f = pool->allocate(7 * MB);
  // free
  pool->free(a, 1 * MB);
  pool->free(b, 9 * MB);
  pool->free(c, 16 * MB);
  pool->free(d, 16 * MB);
  pool->free(e, 16 * MB);
  pool->free(f, 7 * MB);
  ASSERT_EQ(0, pool->currentBytes());
}

TEST_F(MemoryAllocationListenerTest, singleMode) {
  const uint64_t kMaxCap = 128L * MB;
  MemoryManager::Options options = {
      .allocatorCapacity = kDefaultCapacity,
      .singleAllocationThreshold = kDefaultSingleAllocationThreshold,
      .accumulativeAllocationThreshold = 0};
  std::shared_ptr<MemoryManager> manager =
      std::make_shared<MemoryManager>(options);
  auto root = manager->addRootPool("MemoryAllocationListenerTest", kMaxCap);
  auto pool = root->addLeafChild("static_quota", true);
  ASSERT_EQ(0, pool->currentBytes());
  // allocate
  void* a = pool->allocate(1 * MB);
  void* b = pool->allocate(9 * MB);
  void* c = pool->allocate(16 * MB);
  void* d = pool->allocate(16 * MB);
  void* e = pool->allocate(16 * MB);
  void* f = pool->allocate(7 * MB);
  // free
  pool->free(a, 1 * MB);
  pool->free(b, 9 * MB);
  pool->free(c, 16 * MB);
  pool->free(d, 16 * MB);
  pool->free(e, 16 * MB);
  pool->free(f, 7 * MB);
  ASSERT_EQ(0, pool->currentBytes());
}

TEST_F(MemoryAllocationListenerTest, disableMode) {
  const uint64_t kMaxCap = 128L * MB;
  MemoryManager::Options options = {
      .allocatorCapacity = kDefaultCapacity,
      .singleAllocationThreshold = 0,
      .accumulativeAllocationThreshold = 0};
  std::shared_ptr<MemoryManager> manager =
      std::make_shared<MemoryManager>(options);
  auto root = manager->addRootPool("MemoryAllocationListenerTest", kMaxCap);
  auto pool = root->addLeafChild("static_quota", true);
  ASSERT_EQ(0, pool->currentBytes());
  // allocate
  void* a = pool->allocate(1 * MB);
  void* b = pool->allocate(9 * MB);
  void* c = pool->allocate(16 * MB);
  void* d = pool->allocate(16 * MB);
  void* e = pool->allocate(16 * MB);
  void* f = pool->allocate(7 * MB);
  // free
  pool->free(a, 1 * MB);
  pool->free(b, 9 * MB);
  pool->free(c, 16 * MB);
  pool->free(d, 16 * MB);
  pool->free(e, 16 * MB);
  pool->free(f, 7 * MB);
  ASSERT_EQ(0, pool->currentBytes());
}

TEST_F(MemoryAllocationListenerTest, PoolRegex) {
  const uint64_t kMaxCap = 128L * MB;
  MemoryManager::Options options = {
      .allocatorCapacity = kDefaultCapacity,
      .poolRegex = "not_exists",
      .singleAllocationThreshold = kDefaultSingleAllocationThreshold,
      .accumulativeAllocationThreshold =
          kDefaultAccumulativeAllocationThreshold};
  std::shared_ptr<MemoryManager> manager =
      std::make_shared<MemoryManager>(options);
  auto root = manager->addRootPool("MemoryAllocationListenerTest", kMaxCap);
  auto pool = root->addLeafChild("static_quota", true);
  ASSERT_EQ(0, pool->currentBytes());
  // allocate
  void* a = pool->allocate(1 * MB);
  void* b = pool->allocate(9 * MB);
  void* c = pool->allocate(16 * MB);
  void* d = pool->allocate(16 * MB);
  void* e = pool->allocate(16 * MB);
  void* f = pool->allocate(7 * MB);
  // free
  pool->free(a, 1 * MB);
  pool->free(b, 9 * MB);
  pool->free(c, 16 * MB);
  pool->free(d, 16 * MB);
  pool->free(e, 16 * MB);
  pool->free(f, 7 * MB);
  ASSERT_EQ(0, pool->currentBytes());
}

} // namespace memory
} // namespace bolt
} // namespace bytedance
