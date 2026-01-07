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

#include <cstdint>
#include <mutex>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/sparksql/AllocationListener.h"
#include "bolt/common/memory/sparksql/MemoryTarget.h"
#include "bolt/common/memory/sparksql/MemoryUsageStats.h"
namespace bytedance::bolt::memory::sparksql {

ManagedAllocationListener::ManagedAllocationListener(
    const std::string& name,
    MemoryTargetPtr& target,
    SimpleMemoryUsageRecorderPtr& sharedUsage) {
  name_ = name;
  target_ = target;
  sharedUsage_ = sharedUsage;
}

ManagedAllocationListener::~ManagedAllocationListener() {}

int64_t ManagedAllocationListener::reserve(int64_t size) {
  std::lock_guard<std::recursive_mutex> guard(mutex_);
  int64_t granted = target_->borrow(size);
  // no need throw exception, we use return value
  sharedUsage_->inc(granted);
  return granted;
}

int64_t ManagedAllocationListener::unreserve(int64_t size) {
  std::lock_guard<std::recursive_mutex> guard(mutex_);
  int64_t freed = target_->repay(size);
  sharedUsage_->inc(-freed);
  return freed;
}

int64_t ManagedAllocationListener::allocationChanged(int64_t size) {
  if (size > 0) {
    return reserve(size);
  }
  if (size < 0) {
    return unreserve(-size);
  }
  return 0;
}

int64_t ManagedAllocationListener::getUsedBytes() {
  return target_->usedBytes();
}

} // namespace bytedance::bolt::memory::sparksql