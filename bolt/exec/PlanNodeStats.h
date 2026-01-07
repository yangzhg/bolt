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

#include <folly/dynamic.h>
#include "bolt/common/time/CpuWallTimer.h"
#include "bolt/exec/Operator.h"
#include "bolt/expression/ExprStats.h"
namespace bytedance::bolt::exec {
struct TaskStats;
} // namespace bytedance::bolt::exec
namespace bytedance::bolt::exec {

/// Aggregated runtime statistics per plan node.
///
/// Runtime statistics are collected on a per-operator instance basis. There can
/// be multiple operator types and multiple instances of each operator type that
/// correspond to a given plan node. For example, ProjectNode corresponds to
/// a single operator type, FilterProject, but HashJoinNode corresponds to two
/// operator types, HashProbe and HashBuild. Each operator type may have
/// different runtime parallelism, e.g. there can be multiple instances of each
/// operator type. Plan node statistics are calculated by adding up
/// operator-level statistics for all corresponding operator instances.
struct PlanNodeStats {
  explicit PlanNodeStats() = default;

  PlanNodeStats(const PlanNodeStats&) = delete;
  PlanNodeStats& operator=(const PlanNodeStats&) = delete;

  PlanNodeStats(PlanNodeStats&&) = default;
  PlanNodeStats& operator=(PlanNodeStats&&) = default;
  PlanNodeStats& operator+=(const PlanNodeStats&);

  /// Sum of input rows for all corresponding operators. Useful primarily for
  /// leaf plan nodes or plan nodes that correspond to a single operator type.
  uint64_t inputRows{0};

  /// Sum of input batches for all corresponding operators.
  vector_size_t inputVectors{0};

  /// Sum of input bytes for all corresponding operators.
  uint64_t inputBytes{0};

  /// Sum of raw input rows for all corresponding operators. Applies primarily
  /// to TableScan operator which reports rows before pushed down filter as raw
  /// input.
  uint64_t rawInputRows{0};

  /// Sum of raw input bytes for all corresponding operators.
  uint64_t rawInputBytes{0};

  std::vector<uint64_t> rawBytesReads{0, 0, 0, 0};
  std::vector<uint64_t> cntReads{0, 0, 0, 0};
  std::vector<uint64_t> scanTimeReads{0, 0, 0, 0};

  /// Contains the dynamic filters stats if applied.
  DynamicFilterStats dynamicFilterStats;

  /// Sum of output rows for all corresponding operators. When
  /// plan node corresponds to multiple operator types, operators of only one of
  /// these types report non-zero output rows.
  uint64_t outputRows{0};

  /// Sum of output batches for all corresponding operators.
  vector_size_t outputVectors{0};

  /// Sum of output bytes for all corresponding operators.
  uint64_t outputBytes{0};

  // Sum of CPU, scheduled and wall times for isBLocked call for all
  // corresponding operators.
  CpuWallTiming isBlockedTiming;

  // Sum of CPU, scheduled and wall times for addInput call for all
  // corresponding operators.
  CpuWallTiming addInputTiming;

  // Sum of CPU, scheduled and wall times for noMoreInput call for all
  // corresponding operators.
  CpuWallTiming finishTiming;

  // Sum of CPU, scheduled and wall times for getOutput call for all
  // corresponding operators.
  CpuWallTiming getOutputTiming;

  /// Sum of CPU, scheduled and wall times for all corresponding operators. For
  /// each operator, timing of addInput, getOutput and finish calls are added
  /// up.
  CpuWallTiming cpuWallTiming;

  /// Sum of CPU, scheduled and wall times spent on background activities
  /// (activities that are not running on driver threads) for all corresponding
  /// operators.
  CpuWallTiming backgroundTiming;

  /// Sum of blocked wall time for all corresponding operators.
  uint64_t blockedWallNanos{0};

  /// Max of peak memory usage for all corresponding operators. Assumes that all
  /// operator instances were running concurrently.
  uint64_t peakMemoryBytes{0};

  uint64_t numMemoryAllocations{0};

  uint64_t physicalWrittenBytes{0};

  /// Operator-specific counters.
  std::unordered_map<std::string, RuntimeMetric> customStats;

  /// Breakdown of stats by operator type.
  std::unordered_map<std::string, std::unique_ptr<PlanNodeStats>> operatorStats;

  /// Number of drivers that executed the pipeline.
  int numDrivers{0};

  /// Number of total splits.
  int numSplits{0};

  // Total bytes in memory for spilling
  uint64_t spilledInputBytes{0};

  /// Total bytes written for spilling.
  uint64_t spilledBytes{0};

  /// Total rows written for spilling.
  uint64_t spilledRows{0};

  uint64_t spillTotalTime{0};

  uint64_t sortOutputTime{0};
  uint64_t sortColToRowTime{0};
  uint64_t sortInSortTime{0};
  uint64_t windowAddInputTime{0};
  uint64_t windowComputeWindowFunctionTime{0};
  uint64_t windowExtractColumnTime{0};
  uint64_t windowOutputTime{0};
  uint64_t windowHasNextPartitionTime{0};
  uint64_t windowLoadFromSpillTime{0};
  uint64_t windowSpillTime{0};
  uint64_t buildPartitionTime{0};

  /// Total spilled partitions.
  uint32_t spilledPartitions{0};

  /// Total spilled files.
  uint32_t spilledFiles{0};

  /// A map of expression name to its respective stats.
  std::unordered_map<std::string, ExprStats> expressionStats;

  /// Add stats for a single operator instance.
  void add(const OperatorStats& stats);

  std::string toString(bool includeInputStats = false) const;

  bool isMultiOperatorTypeNode() const {
    return operatorStats.size() > 1;
  }

 private:
  void addTotals(const OperatorStats& stats);
};

std::unordered_map<core::PlanNodeId, PlanNodeStats> toPlanStats(
    const TaskStats& taskStats);

folly::dynamic toPlanStatsJson(const bytedance::bolt::exec::TaskStats& stats);

std::vector<uint64_t> getAttributeResultVector(
    const bytedance::bolt::exec::TaskStats& stats,
    const std::string& attribute);

using PlanNodeAnnotation =
    std::function<std::string(const core::PlanNodeId& id)>;

/// Returns human-friendly representation of the plan augmented with runtime
/// statistics. The result has the same plan representation as in
/// PlanNode::toString(true, true), but each plan node includes an additional
/// line with runtime statistics. Plan nodes that correspond to multiple
/// operator types, e.g. HashJoinNode, also include breakdown of runtime
/// statistics per operator type.
///
/// Note that input row counts and sizes are printed only for leaf plan nodes.
///
/// @param includeCustomStats If true, prints operator-specific counters.
/// @param annotation Function to extend plan printing with plugin, for
/// example optimizer estimates next to execution stats.
std::string printPlanWithStats(
    const core::PlanNode& plan,
    const TaskStats& taskStats,
    bool includeCustomStats = false,
    bool withPlanNodeId = false,
    PlanNodeAnnotation annotation = nullptr);
} // namespace bytedance::bolt::exec
