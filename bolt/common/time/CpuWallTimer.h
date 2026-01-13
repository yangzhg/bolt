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

#include <fmt/format.h>
#include <folly/dynamic.h>
#include <chrono>
#include "bolt/common/process/ProcessBase.h"
namespace bytedance::bolt {

// Tracks call count and elapsed CPU and wall time for a repeating operation.
struct CpuWallTiming {
  uint64_t count = 0;
  uint64_t wallNanos = 0;
  uint64_t cpuNanos = 0;

  void add(const CpuWallTiming& other) {
    count += other.count;
    cpuNanos += other.cpuNanos;
    wallNanos += other.wallNanos;
  }

  void clear() {
    count = 0;
    wallNanos = 0;
    cpuNanos = 0;
  }

  std::string toString() const {
    return fmt::format(
        "count: {}, wallNanos: {}, cpuNanos: {}", count, wallNanos, cpuNanos);
  }

  folly::dynamic serialize() const {
    folly::dynamic obj = folly::dynamic::object;
    obj["count"] = count;
    obj["wallNanos"] = wallNanos;
    obj["cpuNanos"] = cpuNanos;
    return obj;
  }
};

// Adds elapsed CPU and wall time to a CpuWallTiming.
class CpuWallTimer {
 public:
  explicit CpuWallTimer(CpuWallTiming& timing);
  ~CpuWallTimer();

 private:
  uint64_t cpuTimeStart_;
  std::chrono::steady_clock::time_point wallTimeStart_;
  CpuWallTiming& timing_;
};

/// Keeps track of elapsed CPU and wall time from construction time.
class DeltaCpuWallTimeStopWatch {
 public:
  explicit DeltaCpuWallTimeStopWatch()
      : wallTimeStart_(std::chrono::steady_clock::now()),
        cpuTimeStart_(process::threadCpuNanos()) {}

  CpuWallTiming elapsed() const {
    // NOTE: End the cpu-time timing first, and then end the wall-time timing,
    // so as to avoid the counter-intuitive phenomenon that the final calculated
    // cpu-time is slightly larger than the wall-time.
    uint64_t cpuTimeDuration = process::threadCpuNanos() - cpuTimeStart_;
    uint64_t wallTimeDuration =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - wallTimeStart_)
            .count();
    return CpuWallTiming{1, wallTimeDuration, cpuTimeDuration};
  }

 private:
  // NOTE: Put `wallTimeStart_` before `cpuTimeStart_`, so that wall-time starts
  // counting earlier than cpu-time.
  const std::chrono::steady_clock::time_point wallTimeStart_;
  const uint64_t cpuTimeStart_;
};

// Composes delta CpuWallTiming upon destruction and passes it to the user
// callback, where it can be added to the user's CpuWallTiming using
// CpuWallTiming::add().
template <typename F>
class DeltaCpuWallTimer {
 public:
  explicit DeltaCpuWallTimer(F&& func) : func_(std::move(func)) {}

  ~DeltaCpuWallTimer() {
    func_(timer_.elapsed());
  }

 private:
  DeltaCpuWallTimeStopWatch timer_;
  F func_;
};

} // namespace bytedance::bolt
