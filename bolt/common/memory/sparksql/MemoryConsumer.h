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

#include <folly/Random.h>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>

#include "bolt/common/memory/sparksql/TaskMemoryManager.h"
namespace bytedance::bolt::memory::sparksql {

class MemoryConsumer {
 public:
  explicit MemoryConsumer(TaskMemoryManagerWeakPtr taskMemoryManager);

  virtual ~MemoryConsumer();

  virtual int64_t getUsed();

  virtual void spill();

  // don't call acquireMemory() from spill()
  virtual int64_t spill(int64_t size) = 0;

  virtual int64_t acquireMemory(int64_t size) = 0;

  virtual void freeMemory(int64_t size) = 0;

  friend std::ostream& operator<<(
      std::ostream& os,
      const MemoryConsumer& consumer);

  friend std::ostream& operator<<(
      std::ostream& os,
      const MemoryConsumer* consumer);

  std::string toString() const;

 protected:
  TaskMemoryManagerWeakPtr taskMemoryManager_;
  int64_t used_;
  std::string uuid_;

 private:
  static std::string genUUID() {
    const int64_t len = 16;
    std::string uuid(len, ' ');
    const char charset[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    for (size_t i = 0; i < len; ++i) {
      uuid[i] = charset[folly::Random::rand32() % (sizeof(charset) - 1)];
    }
    return uuid;
  }
};

} // namespace bytedance::bolt::memory::sparksql
