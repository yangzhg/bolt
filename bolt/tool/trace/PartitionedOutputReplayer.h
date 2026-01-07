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

#include <utility>

#include "bolt/core/PlanNode.h"
#include "bolt/exec/OutputBufferManager.h"
#include "bolt/tool/trace/OperatorReplayerBase.h"
namespace bytedance::bolt::tool::trace {

/// Concurrently gets all partitioned buffer content (vec<IOBuf>) for every
/// partition.
void consumeAllData(
    const std::shared_ptr<exec::OutputBufferManager>& bufferManager,
    const std::string& taskId,
    uint32_t numPartitions,
    folly::Executor* executor,
    folly::ThreadPoolExecutor* consumerExecutor,
    std::function<void(uint32_t, std::unique_ptr<folly::IOBuf>)> consumer);

/// The replayer to replay the traced 'PartitionedOutput' operator.
class PartitionedOutputReplayer final : public OperatorReplayerBase {
 public:
  using ConsumerCallBack =
      std::function<void(uint32_t, std::unique_ptr<folly::IOBuf>)>;

  PartitionedOutputReplayer(
      const std::string& traceDir,
      const std::string& queryId,
      const std::string& taskId,
      const std::string& nodeId,
      const std::string& operatorType,
      const std::string& driverIds,
      uint64_t queryCapacity,
      folly::Executor* executor,
      const ConsumerCallBack& consumerCb = [](auto partition, auto page) {});

  RowVectorPtr run(bool /*unused*/) override;

 private:
  core::PlanNodePtr createPlanNode(
      const core::PlanNode* node,
      const core::PlanNodeId& nodeId,
      const core::PlanNodePtr& source) const override;

  const core::PartitionedOutputNode* const originalNode_;
  const std::shared_ptr<exec::OutputBufferManager> bufferManager_{
      exec::OutputBufferManager::getInstance().lock()};
  const std::unique_ptr<folly::Executor> executor_{
      std::make_unique<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency(),
          std::make_shared<folly::NamedThreadFactory>("Driver"))};
  const ConsumerCallBack consumerCb_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> consumerExecutor_;
};
} // namespace bytedance::bolt::tool::trace
