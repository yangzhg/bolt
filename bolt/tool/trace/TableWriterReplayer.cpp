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

#include <folly/executors/IOThreadPoolExecutor.h>

#include "bolt/exec/TableWriter.h"
#include "bolt/exec/TraceUtil.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/tool/trace/TableWriterReplayer.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
namespace bytedance::bolt::tool::trace {
namespace {

std::shared_ptr<connector::hive::HiveInsertTableHandle>
makeHiveInsertTableHandle(
    const core::TableWriteNode* node,
    std::string targetDir) {
  const auto tracedHandle =
      std::dynamic_pointer_cast<connector::hive::HiveInsertTableHandle>(
          node->insertTableHandle()->connectorInsertTableHandle());
  const auto inputColumns = tracedHandle->inputColumns();
  const auto compressionKind =
      tracedHandle->compressionKind().value_or(common::CompressionKind_NONE);
  const auto storageFormat = tracedHandle->storageFormat();
  const auto serdeParameters = tracedHandle->serdeParameters();
  const auto writerOptions = tracedHandle->writerOptions();
  return std::make_shared<connector::hive::HiveInsertTableHandle>(
      inputColumns,
      std::make_shared<connector::hive::LocationHandle>(
          targetDir,
          targetDir,
          connector::hive::LocationHandle::TableType::kNew),
      storageFormat,
      tracedHandle->bucketProperty() == nullptr
          ? nullptr
          : std::make_shared<connector::hive::HiveBucketProperty>(
                *tracedHandle->bucketProperty()),
      compressionKind,
      std::unordered_map<std::string, std::string>{},
      writerOptions);
}

std::shared_ptr<core::InsertTableHandle> createInsertTableHanlde(
    const std::string& connectorId,
    const core::TableWriteNode* node,
    std::string targetDir) {
  return std::make_shared<core::InsertTableHandle>(
      connectorId, makeHiveInsertTableHandle(node, std::move(targetDir)));
}
} // namespace

core::PlanNodePtr TableWriterReplayer::createPlanNode(
    const core::PlanNode* node,
    const core::PlanNodeId& nodeId,
    const core::PlanNodePtr& source) const {
  const auto* tableWriterNode = dynamic_cast<const core::TableWriteNode*>(node);
  BOLT_CHECK_NOT_NULL(tableWriterNode);
  const auto insertTableHandle =
      createInsertTableHanlde("test-hive", tableWriterNode, replayOutputDir_);
  return std::make_shared<core::TableWriteNode>(
      nodeId,
      tableWriterNode->columns(),
      tableWriterNode->columnNames(),
      tableWriterNode->aggregationNode(),
      insertTableHandle,
      tableWriterNode->hasPartitioningScheme(),
      TableWriteTraits::outputType(tableWriterNode->aggregationNode()),
      tableWriterNode->commitStrategy(),
      source);
}
} // namespace bytedance::bolt::tool::trace
