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

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
namespace bytedance::bolt::memory::sparksql {

struct MemoryUsageStats;
using MemoryUsageStatsPtr = std::shared_ptr<MemoryUsageStats>;

struct MemoryUsageStats {
  int64_t peak;
  int64_t current;
  std::map<std::string, MemoryUsageStatsPtr> children;
};

class MemoryUsageStatsBuilder;
using MemoryUsageStatsBuilderPtr = std::shared_ptr<MemoryUsageStatsBuilder>;

class MemoryUsageStatsBuilder {
 public:
  virtual ~MemoryUsageStatsBuilder() = default;

  virtual MemoryUsageStatsPtr toStats() = 0;
};

class MemoryUsageRecorder;
using MemoryUsageRecorderPtr = std::shared_ptr<MemoryUsageRecorder>;

class MemoryUsageRecorder : public MemoryUsageStatsBuilder {
 public:
  ~MemoryUsageRecorder() override = default;

  virtual void inc(int64_t bytes) = 0;

  // peak used bytes
  virtual int64_t peak() = 0;

  // current used bytes
  virtual int64_t current() = 0;
};

class SimpleMemoryUsageRecorder;
using SimpleMemoryUsageRecorderPtr = std::shared_ptr<SimpleMemoryUsageRecorder>;

class SimpleMemoryUsageRecorder final : public MemoryUsageRecorder {
 public:
  SimpleMemoryUsageRecorder();

  ~SimpleMemoryUsageRecorder() override;

  void inc(int64_t bytes) override;

  int64_t peak() override;

  int64_t current() override;

  MemoryUsageStatsPtr toStats(
      const std::map<std::string, MemoryUsageStatsPtr>& children);

  MemoryUsageStatsPtr toStats() override;

 private:
  std::atomic<int64_t> peak_{};
  std::atomic<int64_t> current_{};
};

} // namespace bytedance::bolt::memory::sparksql