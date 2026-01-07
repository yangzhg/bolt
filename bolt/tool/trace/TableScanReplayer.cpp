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

#include "bolt/tool/trace/TableScanReplayer.h"

#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/exec/OperatorTraceReader.h"
#include "bolt/exec/TraceUtil.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/tool/trace/TraceReplayTaskRunner.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
namespace bytedance::bolt::tool::trace {

RowVectorPtr TableScanReplayer::run(bool copyResults) {
  TraceReplayTaskRunner traceTaskRunner(createPlan(), createQueryCtx());
  auto [task, result] = traceTaskRunner.maxDrivers(driverIds_.size())
                            .splits(replayPlanNodeId_, getSplits())
                            .run(copyResults);
  printStats(task);
  return result;
}

core::PlanNodePtr TableScanReplayer::createPlanNode(
    const core::PlanNode* node,
    const core::PlanNodeId& nodeId,
    const core::PlanNodePtr& /*source*/) const {
  const auto scanNode = dynamic_cast<const core::TableScanNode*>(node);
  BOLT_CHECK_NOT_NULL(scanNode);
  return std::make_shared<core::TableScanNode>(
      nodeId,
      scanNode->outputType(),
      scanNode->tableHandle(),
      scanNode->assignments());
}

std::vector<exec::Split> TableScanReplayer::getSplits() const {
  std::vector<std::string> splitInfoDirs;
  for (const auto driverId : driverIds_) {
    splitInfoDirs.push_back(exec::trace::getOpTraceDirectory(
        nodeTraceDir_, pipelineIds_.front(), driverId));
  }
  const auto splitStrs =
      exec::trace::OperatorTraceSplitReader(
          splitInfoDirs, memory::MemoryManager::getInstance()->tracePool())
          .read();

  std::vector<exec::Split> splits;
  for (const auto& splitStr : splitStrs) {
    folly::dynamic splitInfoObj = folly::parseJson(splitStr);
    const auto split =
        ISerializable::deserialize<connector::hive::HiveConnectorSplit>(
            splitInfoObj);
    splits.emplace_back(
        std::const_pointer_cast<connector::hive::HiveConnectorSplit>(split));
  }
  return splits;
}
} // namespace bytedance::bolt::tool::trace
