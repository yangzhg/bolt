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

#pragma once

#include "bolt/common/memory/MemoryPool.h"
namespace bytedance::bolt::memory {

class MemoryManager;

class MemoryPoolForGluten : public MemoryPoolImpl {
 public:
  MemoryPoolForGluten(
      MemoryManager* manager,
      const std::string& name,
      Kind kind,
      std::shared_ptr<MemoryPool> parent,
      std::unique_ptr<MemoryReclaimer> reclaimer,
      const Options& options = Options{});

  ~MemoryPoolForGluten() override;

  std::shared_ptr<MemoryPool> genChild(
      std::shared_ptr<MemoryPool> parent,
      const std::string& name,
      Kind kind,
      bool threadSafe,
      const std::function<size_t(size_t)>& getPreferredSize,
      std::unique_ptr<MemoryReclaimer> reclaimer) override;

  bool maybeReserve(uint64_t size) override;

  bool maybeReserveThreadSafe(uint64_t size);

  bool maybeReserveIncrementReservationThreadSafe(
      MemoryPool* requestor,
      uint64_t size);

 private:
  int64_t offHeapBytes_;
  bool enableDynamicMemoryQuotaManager_{false};
};

} // namespace bytedance::bolt::memory
