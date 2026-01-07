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
#include <mutex>
#include <queue>
#include <set>
#include <string>

#include <folly/Indestructible.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include "bolt/common/cpu/CpuUsageTracker.h"
#include "bolt/exec/Task.h"
namespace bytedance::bolt::exec {
struct TaskStats;

using TaskQueue = std::queue<std::shared_ptr<Task>>;
enum SchedulerState { kInit, kSampling, kRevising, kCompleted };
std::string mapSchedulerStateToString(const SchedulerState& state);

class ExecutorTaskScheduler {
 public:
  static ExecutorTaskScheduler& instance();

  // return true means task should wait for scheduling
  // false means immediate execution
  bool waitForScheduling(std::shared_ptr<Task>& task, bool doSchedule);

  // task is terminated and schedule new tasks waiting in taskQueue_
  void scheduleNewTasksIfAny(
      const std::shared_ptr<Task>& task,
      bool terminateFinished,
      bool doSchedule);

  void setDefaultTaskParallelism(int32_t defaultConcurrency) {
    defaultConcurrency_ = defaultConcurrency;
    currentConcurrency_ = defaultConcurrency;
    VLOG(1) << __FUNCTION__ << " to " << defaultConcurrency;
  }

  void setTargetMemoryUsage(double cpuUsage) {
    targetCpuUsage_ = cpuUsage;
  }

  void setIOThreadInfo(
      double numPerTask,
      int32_t defaultIOThreadNum,
      int32_t maxIOThreadNum,
      folly::IOThreadPoolExecutor* executor) {
    ioThreadPerTask_ = numPerTask;
    defaultIOThreadNum_ = defaultIOThreadNum;
    maxIoThreadNum_ = maxIOThreadNum;
    ioExecutor_ = executor;
    LOG(INFO) << __FUNCTION__ << " set ioThreadPerTask_  to "
              << ioThreadPerTask_ << ", defaultIOThreadNum_ to "
              << defaultIOThreadNum << ", maxIOThreadNum_ to "
              << maxIOThreadNum;
  }

  int32_t getCurrentConcurrency() const {
    return currentConcurrency_;
  }

  int32_t getConcurrencyVersion() const {
    return concurrencyVersion_;
  }

  // construct from TaskStats
  struct SimplifiedTaskStats {
    uint32_t memoryReclaimCount{0};
    uint64_t peakMemoryBytes{0};
    double memoryUsedRatio{0.0};
    uint64_t numSourceRows{0};
    uint64_t executionTimeMs{0};
  };

  struct MemoryUsageStats {
    uint64_t rssBytes{0};
    uint64_t vssBytes{0};
  };

  struct ExecutorRuntimeStatsCollector {
    void addRuntimeStats(const SimplifiedTaskStats& taskStats);
    void clear() {
      accumulatedMemoryReclaimCount_ = 0;
      accumulatedMemoryUsedRatio_ = 0.0;
      accumulatedNumSourceRows_ = 0;
      accumulatedExecutionTimeMs_ = 0;
    }

    int32_t accumulatedMemoryReclaimCount_{0};
    double accumulatedMemoryUsedRatio_{0.0};
    uint64_t accumulatedNumSourceRows_{0};
    uint64_t accumulatedExecutionTimeMs_{0};
  };

 private:
  double targetCpuUsage_ = 4.0;
  static constexpr int32_t DefaultTrackingTaskCount = 1;

  // merge taskStats
  void collectTaskStatsUnlocked(
      const TaskStats& taskStats,
      SimplifiedTaskStats& stats);

  void reportRuntimeStatsLocked(
      SimplifiedTaskStats& stats,
      const MemoryUsageStats& memoryStats);

  // recalculate concurrency
  void decideConcurrencyLocked();

  // estimate concurrency multiplier based on
  // cpu and memory usage
  double estimatedMulOnResource();

  // start to track tasks runtime stats
  void startTaskTracking(const std::string& taskId);

  std::mutex mutex_;
  std::atomic_int32_t numRunningTask_{0};
  // concurrency by default
  int32_t defaultConcurrency_{4};
  // dynamically calculated concurrency
  std::atomic<int32_t> currentConcurrency_{2};
  std::atomic<int32_t> concurrencyVersion_{0};

  // finished tasks during kSampling/kRevising phase
  int32_t numFinishedTask_{0};

  // track task's runtime stats for kSampling or kRevising
  int32_t numTrackingTaskThreshold_{0};

  // time when start sampling or revising
  size_t monitorStartTs_{0};
  double inputThroughput_{0.0};

  // async prefetch io
  double ioThreadPerTask_{4.0};
  int32_t defaultIOThreadNum_{24};
  int32_t maxIoThreadNum_{64};
  folly::IOThreadPoolExecutor* ioExecutor_{nullptr};

  bool retrySampling_{false};

  SchedulerState state_{SchedulerState::kInit};
  TaskQueue taskQueue_;
  // several tasks may have the same taskid
  // use tasksIds_ for deduplication of running spark task count
  folly::Synchronized<std::set<std::string>> tasksIds_;
  // indicate new stage or not
  folly::Synchronized<std::set<int32_t>> stageIds_;

  folly::Synchronized<
      std::unordered_map<std::string, std::vector<std::weak_ptr<Task>>>>
      allTasks_;
  std::set<int32_t> trackingStageIds_;
  cpu::CpuUsageTracker cpuTracker_;

  ExecutorRuntimeStatsCollector statsCollector_;
};

} // namespace bytedance::bolt::exec
