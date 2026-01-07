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

#include "bolt/exec/TaskTraceWriter.h"
#include "bolt/common/file/File.h"
#include "bolt/core/PlanNode.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/exec/Trace.h"
#include "bolt/exec/TraceUtil.h"
namespace bytedance::bolt::exec::trace {

TaskTraceMetadataWriter::TaskTraceMetadataWriter(
    std::string traceDir,
    memory::MemoryPool* pool)
    : traceDir_(std::move(traceDir)),
      fs_(filesystems::getFileSystem(traceDir_, nullptr)),
      traceFilePath_(getTaskTraceMetaFilePath(traceDir_)),
      pool_(pool) {
  BOLT_CHECK_NOT_NULL(fs_);
  BOLT_CHECK(!fs_->exists(traceFilePath_));
}

void TaskTraceMetadataWriter::write(
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    const core::PlanNodePtr& planNode) {
  BOLT_CHECK(!finished_, "Query metadata can only be written once");
  finished_ = true;
  folly::dynamic queryConfigObj = folly::dynamic::object;
  const auto configValues = queryCtx->queryConfig().rawConfigsCopy();
  for (const auto& [key, value] : configValues) {
    queryConfigObj[key] = value;
  }

  folly::dynamic connectorPropertiesObj = folly::dynamic::object;
  for (const auto& [connectorId, configs] :
       queryCtx->connectorSessionProperties()) {
    folly::dynamic obj = folly::dynamic::object;
    for (const auto& [key, value] : configs->rawConfigsCopy()) {
      obj[key] = value;
    }
    connectorPropertiesObj[connectorId] = obj;
  }

  folly::dynamic metaObj = folly::dynamic::object;
  metaObj[TraceTraits::kQueryConfigKey] = queryConfigObj;
  metaObj[TraceTraits::kConnectorPropertiesKey] = connectorPropertiesObj;
  metaObj[TraceTraits::kPlanNodeKey] = planNode->serialize();

  const auto metaStr = folly::toJson(metaObj);
  const auto file = fs_->openFileForWrite(traceFilePath_);
  file->append(metaStr);
  file->close();
}

} // namespace bytedance::bolt::exec::trace
