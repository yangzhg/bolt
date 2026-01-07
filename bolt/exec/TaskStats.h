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

#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "bolt/exec/Driver.h"
#include "bolt/exec/OutputBuffer.h"
namespace bytedance::bolt::exec {

struct OperatorStats;

/// Stores execution stats per pipeline.
struct PipelineStats {
  // Cumulative OperatorStats for finished Drivers. The subscript is the
  // operator id, which is the initial ordinal position of the
  // operator in the DriverFactory.
  std::vector<OperatorStats> operatorStats;

  // True if contains the source node for the task.
  bool inputPipeline;

  // True if contains the sync node for the task.
  bool outputPipeline;

  // [morsel] True if the pipeline is morsel-driven
  bool morselDrivenPipeline;

  double pipelineInputSizeCoefficientOfVariation{0};

  PipelineStats(
      bool _inputPipeline,
      bool _outputPipeline,
      bool _morselDrivenPipeline)
      : inputPipeline{_inputPipeline},
        outputPipeline{_outputPipeline},
        morselDrivenPipeline{_morselDrivenPipeline} {}

  folly::dynamic serialize() const {
    folly::dynamic obj = folly::dynamic::object;
    obj["inputPipeline"] = inputPipeline;
    obj["outputPipeline"] = outputPipeline;
    obj["morselDrivenPipeline"] = morselDrivenPipeline;
    obj["pipelineInputSizeCoefficientOfVariation"] =
        pipelineInputSizeCoefficientOfVariation;
    obj["operatorStats"] = folly::dynamic::array;
    for (const auto& operatorStats : operatorStats) {
      obj["operatorStats"].push_back(operatorStats.serialize());
    }
    return obj;
  }
};

/// Stores execution stats per task.
struct TaskStats {
  int32_t numTotalSplits{0};
  int32_t numFinishedSplits{0};
  int32_t numRunningSplits{0};
  int32_t numQueuedSplits{0};
  std::unordered_set<int32_t> completedSplitGroups;

  /// The subscript is given by each Operator's
  /// DriverCtx::pipelineId. This is a sum total reflecting fully
  /// processed Splits for Drivers of this pipeline.
  std::vector<PipelineStats> pipelineStats;

  /// Epoch time (ms) when task starts to run
  uint64_t executionStartTimeMs{0};

  /// Epoch time (ms) when last split is processed. For some tasks there might
  /// be some additional time to send buffered results before the task finishes.
  uint64_t executionEndTimeMs{0};

  /// Epoch time (ms) when first split is fetched from the task by an operator.
  uint64_t firstSplitStartTimeMs{0};

  /// Epoch time (ms) when last split is fetched from the task by an operator.
  uint64_t lastSplitStartTimeMs{0};

  /// Epoch time (ms) when the task completed, e.g. all splits were processed
  /// and results have been consumed.
  uint64_t endTimeMs{0};

  /// Epoch time (ms) when the task was terminated, i.e. its terminal state
  /// has been set, whether by finishing successfully or with an error, or
  /// being cancelled or aborted.
  uint64_t terminationTimeMs{0};

  /// Let the pipeline skewnesses = P1, P2, P3...
  /// Let the "weight" of each pipeline be W1, W2, W3.
  /// Then the "taskInputSizeCoefficientOfVariation"
  /// = (W1P1 + W2P2 + W3P3) / (W1 + W2 + W3)
  double taskInputSizeCoefficientOfVariation{0};

  /// Total number of drivers.
  uint64_t numTotalDrivers{0};
  /// The number of completed drivers (which slots are null in Task 'drivers_'
  /// list).
  uint64_t numCompletedDrivers{0};
  /// The number of drivers that are terminating or terminated (isTerminated()
  /// returns true).
  uint64_t numTerminatedDrivers{0};
  /// The number of drivers that are currently running on driver thread.
  uint64_t numRunningDrivers{0};
  /// Drivers blocked for various reasons. Based on enum BlockingReason.
  std::unordered_map<BlockingReason, uint64_t> numBlockedDrivers;

  /// Output buffer's memory utilization ratio measured as
  /// current buffer usage / max buffer size
  double outputBufferUtilization{0};
  /// Indicates if output buffer is over-utilized and thus blocks the producers.
  bool outputBufferOverutilized{false};

  /// Output buffer stats if present.
  std::optional<OutputBuffer::Stats> outputBufferStats;

  /// The longest still running operator call in "op::call" format.
  std::string longestRunningOpCall;
  /// The longest still running operator call's duration in ms.
  size_t longestRunningOpCallMs{0};

  /// The total memory reclamation count.
  uint32_t memoryReclaimCount{0};
  /// The total memory reclamation time.
  uint64_t memoryReclaimMs{0};

  /// true means task contains tablescan or shuffle reader
  bool isSourceFragment_{false};

  /// for multi_hive_split
  std::unordered_map<std::string, int32_t> multiSplitUuidToSingleSplitNum;

  void updateMultiSplitInnerNum(const std::string& uuid, int32_t num) {
    multiSplitUuidToSingleSplitNum.try_emplace(uuid, num);
  }

  folly::dynamic serialize() const {
    folly::dynamic obj = folly::dynamic::object;
    obj["numTotalSplits"] = numTotalSplits;
    obj["numFinishedSplits"] = numFinishedSplits;
    obj["numRunningSplits"] = numRunningSplits;
    obj["numQueuedSplits"] = numQueuedSplits;
    obj["completedSplitGroups"] = folly::dynamic::array;
    for (auto group : completedSplitGroups) {
      obj["completedSplitGroups"].push_back(group);
    }
    obj["pipelineStats"] = folly::dynamic::array;
    for (const auto& pipeline : pipelineStats) {
      folly::dynamic pipelineObj = folly::dynamic::object;
      pipelineObj["inputPipeline"] = pipeline.inputPipeline;
      pipelineObj["outputPipeline"] = pipeline.outputPipeline;
      pipelineObj["operatorStats"] = folly::dynamic::array;
      for (const auto& operatorStats : pipeline.operatorStats) {
        pipelineObj["operatorStats"].push_back(operatorStats.serialize());
      }
      pipelineObj["pipelineInputSizeCoefficientOfVariation"] =
          pipeline.pipelineInputSizeCoefficientOfVariation;
      obj["pipelineStats"].push_back(pipelineObj);
    }
    obj["executionStartTimeMs"] = executionStartTimeMs;
    obj["executionEndTimeMs"] = executionEndTimeMs;
    obj["firstSplitStartTimeMs"] = firstSplitStartTimeMs;
    obj["lastSplitStartTimeMs"] = lastSplitStartTimeMs;
    obj["endTimeMs"] = endTimeMs;
    obj["terminationTimeMs"] = terminationTimeMs;
    obj["taskInputSizeCoefficientOfVariation"] =
        taskInputSizeCoefficientOfVariation;
    obj["numTotalDrivers"] = numTotalDrivers;
    obj["numCompletedDrivers"] = numCompletedDrivers;
    obj["numTerminatedDrivers"] = numTerminatedDrivers;
    obj["numRunningDrivers"] = numRunningDrivers;
    obj["numBlockedDrivers"] = folly::dynamic::object;
    for (const auto& [reason, count] : numBlockedDrivers) {
      obj["numBlockedDrivers"][blockingReasonToString(reason)] = count;
    }
    obj["outputBufferUtilization"] = outputBufferUtilization;
    obj["outputBufferOverutilized"] = outputBufferOverutilized;
    obj["longestRunningOpCall"] = longestRunningOpCall;
    obj["longestRunningOpCallMs"] = longestRunningOpCallMs;
    obj["memoryReclaimCount"] = memoryReclaimCount;
    obj["memoryReclaimMs"] = memoryReclaimMs;
    obj["multiSplitUuidToSingleSplitNum"] = folly::dynamic::object;
    for (const auto& [uuid, num] : multiSplitUuidToSingleSplitNum) {
      obj["multiSplitUuidToSingleSplitNum"][uuid] = num;
    }
    return obj;
  }
};

} // namespace bytedance::bolt::exec
