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

#include <cstdint>
#include <memory>
#include <mutex>

#include "bolt/common/memory/sparksql/MemoryTarget.h"
#include "bolt/common/memory/sparksql/MemoryUsageStats.h"
namespace bytedance::bolt::memory::sparksql {

class AllocationListener;
using AllocationListenerPtr = std::shared_ptr<AllocationListener>;

class AllocationListener {
 public:
  virtual ~AllocationListener() = default;

  virtual int64_t allocationChanged(int64_t size) = 0;

  virtual int64_t getUsedBytes() = 0;
};

class ManagedAllocationListener;
using ManagedAllocationListenerPtr = std::shared_ptr<ManagedAllocationListener>;

class ManagedAllocationListener final : public AllocationListener {
 public:
  ManagedAllocationListener(
      const std::string& name,
      MemoryTargetPtr& target,
      SimpleMemoryUsageRecorderPtr& sharedUsage);

  ~ManagedAllocationListener() override;

  int64_t allocationChanged(int64_t size) override;

  int64_t getUsedBytes() override;

  int64_t reserve(int64_t size);

  int64_t unreserve(int64_t size);

 private:
  std::string name_;
  MemoryTargetPtr target_;
  SimpleMemoryUsageRecorderPtr sharedUsage_;
  std::recursive_mutex mutex_;
};

} // namespace bytedance::bolt::memory::sparksql