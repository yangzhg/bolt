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

#include "bolt/exec/MemoryReclaimer.h"
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::memory;

class MemoryReclaimerTest : public OperatorTestBase {
 protected:
  MemoryReclaimerTest() : pool_(memory::memoryManager()->addLeafPool()) {
    const auto seed =
        std::chrono::system_clock::now().time_since_epoch().count();
    rng_.seed(seed);
    LOG(INFO) << "Random seed: " << seed;

    rowType_ = ROW({"c0", "c1"}, {{INTEGER(), VARCHAR()}});
    VectorFuzzer fuzzer{{}, pool_.get()};

    std::vector<RowVectorPtr> values = {fuzzer.fuzzRow(rowType_)};
    core::PlanFragment fakePlanFragment;
    const core::PlanNodeId id{"0"};
    fakePlanFragment.planNode = std::make_shared<core::ValuesNode>(id, values);

    fakeTask_ = Task::create(
        "MemoryReclaimerTest",
        std::move(fakePlanFragment),
        0,
        core::QueryCtx::create(executor_.get()),
        Task::ExecutionMode::kParallel);
  }

  void SetUp() override {}

  void TearDown() override {}

  std::shared_ptr<folly::CPUThreadPoolExecutor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(4)};

  const std::shared_ptr<memory::MemoryPool> pool_;
  RowTypePtr rowType_;
  std::shared_ptr<Task> fakeTask_;
  folly::Random::DefaultGenerator rng_;
};

TEST_F(MemoryReclaimerTest, enterArbitrationTest) {
  for (const auto& underDriverContext : {false, true}) {
    SCOPED_TRACE(fmt::format("underDriverContext: {}", underDriverContext));

    auto reclaimer = exec::MemoryReclaimer::create();
    auto driver = Driver::testingCreate(
        std::make_unique<DriverCtx>(fakeTask_, 0, 0, 0, 0));
    fakeTask_->testingIncrementThreads();
    if (underDriverContext) {
      driver->state().setThread();
      ScopedDriverThreadContext scopedDriverThreadCtx{driver->driverCtx()};
      reclaimer->enterArbitration();
      ASSERT_TRUE(driver->state().isOnThread());
      ASSERT_TRUE(driver->state().suspended());
      reclaimer->leaveArbitration();
      ASSERT_TRUE(driver->state().isOnThread());
      ASSERT_FALSE(driver->state().suspended());
    } else {
      reclaimer->enterArbitration();
      ASSERT_FALSE(driver->state().isOnThread());
      ASSERT_FALSE(driver->state().suspended());
      reclaimer->leaveArbitration();
    }
  }
}

TEST_F(MemoryReclaimerTest, abortTest) {
  for (const auto& leafPool : {false, true}) {
    const std::string testName = fmt::format("leafPool: {}", leafPool);
    SCOPED_TRACE(testName);
    auto rootPool = memory::memoryManager()->addRootPool(
        testName, kMaxMemory, exec::MemoryReclaimer::create());
    ASSERT_FALSE(rootPool->aborted());
    if (leafPool) {
      auto leafPool = rootPool->addLeafChild(
          "leafAbortTest", true, exec::MemoryReclaimer::create());
      try {
        BOLT_FAIL("abortTest error");
      } catch (const BoltRuntimeError& e) {
        leafPool->abort(std::current_exception());
      }
      ASSERT_TRUE(rootPool->aborted());
      ASSERT_TRUE(leafPool->aborted());
    } else {
      auto aggregatePool = rootPool->addAggregateChild(
          "nonLeafAbortTest", exec::MemoryReclaimer::create());
      try {
        BOLT_FAIL("abortTest error");
      } catch (const BoltRuntimeError& e) {
        aggregatePool->abort(std::current_exception());
      }
      ASSERT_TRUE(rootPool->aborted());
      ASSERT_TRUE(aggregatePool->aborted());
    }
  }
}

TEST(ReclaimableSectionGuard, basic) {
  tsan_atomic<bool> nonReclaimableSection{false};
  {
    memory::NonReclaimableSectionGuard guard(&nonReclaimableSection);
    ASSERT_TRUE(nonReclaimableSection);
    {
      memory::ReclaimableSectionGuard guard(&nonReclaimableSection);
      ASSERT_FALSE(nonReclaimableSection);
      {
        memory::ReclaimableSectionGuard guard(&nonReclaimableSection);
        ASSERT_FALSE(nonReclaimableSection);
        {
          memory::NonReclaimableSectionGuard guard(&nonReclaimableSection);
          ASSERT_TRUE(nonReclaimableSection);
        }
        ASSERT_FALSE(nonReclaimableSection);
      }
      ASSERT_FALSE(nonReclaimableSection);
    }
    ASSERT_TRUE(nonReclaimableSection);
  }
  ASSERT_FALSE(nonReclaimableSection);
  nonReclaimableSection = true;
  {
    memory::ReclaimableSectionGuard guard(&nonReclaimableSection);
    ASSERT_FALSE(nonReclaimableSection);
    {
      memory::NonReclaimableSectionGuard guard(&nonReclaimableSection);
      ASSERT_TRUE(nonReclaimableSection);
      {
        memory::ReclaimableSectionGuard guard(&nonReclaimableSection);
        ASSERT_FALSE(nonReclaimableSection);
        {
          memory::ReclaimableSectionGuard guard(&nonReclaimableSection);
          ASSERT_FALSE(nonReclaimableSection);
        }
        ASSERT_FALSE(nonReclaimableSection);
        {
          memory::NonReclaimableSectionGuard guard(&nonReclaimableSection);
          ASSERT_TRUE(nonReclaimableSection);
        }
        ASSERT_FALSE(nonReclaimableSection);
      }
      ASSERT_TRUE(nonReclaimableSection);
    }
    ASSERT_FALSE(nonReclaimableSection);
  }
  ASSERT_TRUE(nonReclaimableSection);
}
