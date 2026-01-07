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

#pragma once

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/Portability.h"
#include "bolt/common/memory/MemoryArbitrator.h"
namespace bytedance::bolt::exec {
/// Provides the default memory reclaimer implementation for bolt task
/// execution.
class MemoryReclaimer : public memory::MemoryReclaimer {
 public:
  virtual ~MemoryReclaimer() = default;

  static std::unique_ptr<memory::MemoryReclaimer> create(int32_t priority = 0);

  void enterArbitration() override;

  void leaveArbitration() noexcept override;

  void abort(memory::MemoryPool* pool, const std::exception_ptr& error)
      override;

 protected:
  MemoryReclaimer(int32_t priortity);
};

/// Callback used by memory arbitration to check if a driver thread under memory
/// arbitration has been put in suspension state. This is to prevent arbitration
/// deadlock as the arbitrator might reclaim memory from the task of the driver
/// thread which is under arbitration. The task reclaim needs to wait for the
/// drivers to go off thread. A suspended driver thread is not counted as
/// running.
void memoryArbitrationStateCheck(memory::MemoryPool& pool);

} // namespace bytedance::bolt::exec
