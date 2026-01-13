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
#include <set>
#include <sstream>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
namespace bytedance::bolt::memory::sparksql {

class MemoryConsumer;
using MemoryConsumerPtr = std::shared_ptr<MemoryConsumer>;
using MemoryConsumerWeakPtr = std::weak_ptr<MemoryConsumer>;

class TaskMemoryManager;
using TaskMemoryManagerPtr = std::shared_ptr<TaskMemoryManager>;
using TaskMemoryManagerWeakPtr = std::weak_ptr<TaskMemoryManager>;

using MemoryRecursiveMutex = std::recursive_mutex;
using MemoryRecursiveMutexGuard = std::unique_lock<MemoryRecursiveMutex>;

class TaskMemoryManager final
    : public std::enable_shared_from_this<TaskMemoryManager> {
 public:
  TaskMemoryManager(
      ExecutionMemoryPoolWeakPtr memoryPool,
      int64_t taskAttemptId);

  ~TaskMemoryManager();

  int64_t acquireExecutionMemory(
      int64_t required,
      MemoryConsumerWeakPtr requestingConsumer);

  int64_t releaseExecutionMemory(int64_t size, MemoryConsumerWeakPtr consumer);

  int64_t getTaskAttemptId() const {
    return taskAttemptId_;
  }

  // TaskMemoryManager must hold child MemoryConsumer's ownership
  void registerConsumer(MemoryConsumerPtr& consumer);

  friend std::ostream& operator<<(
      std::ostream& os,
      const TaskMemoryManager& taskMemoryManager);

  friend std::ostream& operator<<(
      std::ostream& os,
      const TaskMemoryManager* taskMemoryManager);

  std::string toString() const;

 private:
  int64_t taskAttemptId_;

  // ownershipHolder_ is being used to hold some consumer's ownership
  std::set<MemoryConsumerPtr> ownershipHolder_;

  ExecutionMemoryPoolWeakPtr offHeapExecutionMemoryPool_;
  MemoryRecursiveMutex lock_;

  void* historyRequestConsumer_{nullptr};
};

} // namespace bytedance::bolt::memory::sparksql
