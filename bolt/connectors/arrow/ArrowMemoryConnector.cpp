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

#include "bolt/connectors/arrow/ArrowMemoryConnector.h"
#include "bolt/common/base/BoltException.h"
#include "bolt/vector/FlatVector.h"

#include <iostream>
namespace bytedance::bolt::connector::arrow {

ArrowMemoryDataSource::ArrowMemoryDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    bolt::memory::MemoryPool* FOLLY_NONNULL pool)
    : pool_(pool), outputType_(outputType) {}

void ArrowMemoryDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  BOLT_CHECK(
      currentSplit_ == nullptr,
      "Previous split has not been processed yet. Call next() to process the split.");

  currentSplit_ = std::dynamic_pointer_cast<ArrowMemoryConnectorSplit>(split);
}

// Arrow Memory Connector's next() function eats the entire materialized split
// directly. Especially, it takes the advantage of the zero copy benefit between
// Arrow and Bolt vector on primitive types. The Arrow split size should be
// controlled in the Arrow source part.
std::optional<RowVectorPtr> ArrowMemoryDataSource::next(
    uint64_t,
    bolt::ContinueFuture& future) {
  BOLT_CHECK_NOT_NULL(
      currentSplit_.get(), "No split to process. Call addSplit() first.");

  // If the split is exhausted.
  if (currentSplit_->getArrowSplitFinishStatus()) {
    currentSplit_ = nullptr;
    return nullptr;
  }
  auto arrowSplitData = currentSplit_->getArrowSplitData();
  RowVectorPtr nextRowVec = nullptr;
  int rowSize = 0;

  if (!arrowSplitData.inColumns_) {
    try {
      auto vector = importFromArrowAsOwner(
          arrowSplitData.arrowSchemas_[0],
          arrowSplitData.arrowArrays_[0],
          options_,
          pool_);

      currentSplit_->setArrowSplitFinished();
      BOLT_CHECK(
          vector->type()->isRow(),
          "Arrow Split Data must be in struct/row type");
      BOLT_CHECK(
          vector->type()->kindEquals(outputType_),
          "input data type must be same as registered table scan type");
      rowSize = vector->size();
      nextRowVec = std::make_shared<RowVector>(
          pool_,
          outputType_,
          vector->nulls(),
          rowSize,
          std::dynamic_pointer_cast<RowVector>(vector)->children(),
          vector->getNullCount());
    } catch (const BoltException&) {
      currentSplit_->setArrowSplitFinished();
      throw;
    }
  } else {
    // Arrow Schema and Array size are guaranteed to be equal
    int columnSize = arrowSplitData.arrowSchemas_.size();
    std::vector<VectorPtr> columns;
    for (int i = 0; i < columnSize; i++) {
      try {
        auto vector = importFromArrowAsOwner(
            arrowSplitData.arrowSchemas_[i],
            arrowSplitData.arrowArrays_[i],
            options_,
            pool_);
        BOLT_CHECK(
            vector->type()->kindEquals(outputType_->childAt(i)),
            "input data type must be same as registered table scan type");
        columns.push_back(vector);
      } catch (const BoltException&) {
        // To make sure that the java imported arrays are released, we need to
        // process the rest of the columns even if it'll fail.
        for (int j = i + 1; j < columnSize; j++) {
          try {
            auto vector = importFromArrowAsOwner(
                arrowSplitData.arrowSchemas_[j],
                arrowSplitData.arrowArrays_[j],
                options_,
                pool_);
          } catch (const BoltException&) {
            // Ignore exception, use the first memory leak error.
          }
        }
        currentSplit_->setArrowSplitFinished();
        throw;
      }
    }

    currentSplit_->setArrowSplitFinished();
    rowSize = columns[0]->size();
    for (auto vec : columns) {
      BOLT_CHECK_EQ(
          vec->size(),
          rowSize,
          "The row size of each column should be the same")
    }
    nextRowVec = std::make_shared<RowVector>(
        pool_, outputType_, nullptr, rowSize, columns);
  }
  completedRows_ += rowSize;
  return nextRowVec;
}

std::unique_ptr<DataSource> ArrowMemoryConnector::createDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    std::shared_ptr<ConnectorQueryCtx> connectorQueryCtx,
    const core::QueryConfig& queryConfig) {
  return std::make_unique<ArrowMemoryDataSource>(
      outputType, tableHandle, columnHandles, connectorQueryCtx->memoryPool());
}

// BOLT_REGISTER_CONNECTOR_FACTORY(
//     std::make_shared<ArrowMemoryConnectorFactory>())

namespace {
static bool FB_ANONYMOUS_VARIABLE(g_ConnectorFactory) =
    CheckArrowMemoryConnectorFactoryInit<ArrowMemoryConnectorFactory>();
} // namespace

} // namespace bytedance::bolt::connector::arrow
