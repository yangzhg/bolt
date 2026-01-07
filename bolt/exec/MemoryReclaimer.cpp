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

#include "bolt/exec/Driver.h"
#include "bolt/exec/Task.h"
namespace bytedance::bolt::exec {
MemoryReclaimer::MemoryReclaimer(int32_t priority)
    : memory::MemoryReclaimer(priority) {}

std::unique_ptr<memory::MemoryReclaimer> MemoryReclaimer::create(
    int32_t priority) {
  return std::unique_ptr<memory::MemoryReclaimer>(
      new MemoryReclaimer(priority));
}

void MemoryReclaimer::enterArbitration() {
  DriverThreadContext* driverThreadCtx = driverThreadContext();
  if (FOLLY_UNLIKELY(driverThreadCtx == nullptr)) {
    // Skips the driver suspension handling if this memory arbitration
    // request is not issued from a driver thread.
    return;
  }

  Driver* const driver = driverThreadCtx->driverCtx()->driver;
  if (driver->task()->enterSuspended(driver->state()) != StopReason::kNone) {
    // There is no need for arbitration if the associated task has already
    // terminated.
    BOLT_FAIL("Terminate detected when entering suspension");
  }
}

void MemoryReclaimer::leaveArbitration() noexcept {
  DriverThreadContext* driverThreadCtx = driverThreadContext();
  if (FOLLY_UNLIKELY(driverThreadCtx == nullptr)) {
    // Skips the driver suspension handling if this memory arbitration
    // request is not issued from a driver thread.
    return;
  }
  Driver* const driver = driverThreadCtx->driverCtx()->driver;
  driver->task()->leaveSuspended(driver->state());
}

void MemoryReclaimer::abort(
    memory::MemoryPool* pool,
    const std::exception_ptr& error) {
  if (pool->kind() == memory::MemoryPool::Kind::kLeaf) {
    return;
  }
  pool->visitChildren([&](memory::MemoryPool* child) {
    auto* reclaimer = child->reclaimer();
    if (reclaimer != nullptr) {
      reclaimer->abort(child, error);
    }
    return true;
  });
}

void memoryArbitrationStateCheck(memory::MemoryPool& pool) {
  const auto* driverThreadCtx = driverThreadContext();
  if (driverThreadCtx != nullptr) {
    Driver* driver = driverThreadCtx->driverCtx()->driver;
    if (!driver->state().suspended()) {
      BOLT_FAIL(
          "Driver thread is not suspended under memory arbitration processing: {}, request memory pool: {}",
          driver->toString(),
          pool.name());
    }
  }
}
} // namespace bytedance::bolt::exec
