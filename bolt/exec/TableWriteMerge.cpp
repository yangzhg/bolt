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

#include "bolt/exec/TableWriteMerge.h"

#include "HashAggregation.h"
#include "bolt/exec/TableWriter.h"
#include "bolt/exec/Task.h"
namespace bytedance::bolt::exec {
namespace {
bool isSameCommitContext(
    const folly::dynamic& first,
    const folly::dynamic& second) {
  return std::tie(
             first[TableWriteTraits::kTaskIdContextKey],
             first[TableWriteTraits::kCommitStrategyContextKey]) ==
      std::tie(
             second[TableWriteTraits::kTaskIdContextKey],
             second[TableWriteTraits::kCommitStrategyContextKey]);
}

bool containsNonNullRows(const VectorPtr& vector) {
  if (!vector->mayHaveNulls()) {
    return true;
  }
  for (int i = 0; i < vector->size(); ++i) {
    if (!vector->isNullAt(i)) {
      return true;
    }
  }
  return false;
}
} // namespace

TableWriteMerge::TableWriteMerge(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::TableWriteMergeNode>& tableWriteMergeNode)
    : Operator(
          driverCtx,
          tableWriteMergeNode->outputType(),
          operatorId,
          tableWriteMergeNode->id(),
          "TableWriteMerge") {
  BOLT_USER_CHECK(outputType_->equivalent(
      *TableWriteTraits::outputType(tableWriteMergeNode->aggregationNode())));
  if (tableWriteMergeNode->aggregationNode() != nullptr) {
    aggregation_ = std::make_unique<HashAggregation>(
        operatorId, driverCtx, tableWriteMergeNode->aggregationNode());
  }
}

void TableWriteMerge::initialize() {
  Operator::initialize();
  if (aggregation_ != nullptr) {
    aggregation_->initialize();
  }
}

void TableWriteMerge::addInput(RowVectorPtr input) {
  BOLT_CHECK(!noMoreInput_);
  BOLT_CHECK_GT(input->size(), 0);

  if (isStatistics(input)) {
    BOLT_CHECK_NOT_NULL(aggregation_);
    aggregation_->addInput(input);
    return;
  }

  // Increments row count.
  numRows_ += TableWriteTraits::getRowCount(input);

  // Makes sure the lifespan is the same.
  auto commitContext = TableWriteTraits::getTableCommitContext(input);
  if (lastCommitContext_ != nullptr) {
    BOLT_CHECK(
        isSameCommitContext(lastCommitContext_, commitContext),
        "incompatible table commit context: {} is not compatible with {}",
        lastCommitContext_.asString(),
        commitContext.asString());
  }
  lastCommitContext_ = commitContext;

  // Adds fragments to the buffer. Fragments will be emitted as soon as possible
  // to avoid using extra memory.
  auto fragmentVector = input->childAt(TableWriteTraits::kFragmentChannel);
  if (containsNonNullRows(fragmentVector)) {
    fragmentVectors_.push(fragmentVector);
  }
}

void TableWriteMerge::noMoreInput() {
  Operator::noMoreInput();
  if (aggregation_ != nullptr) {
    aggregation_->noMoreInput();
  }
  close();
}

RowVectorPtr TableWriteMerge::getOutput() {
  // Passes through fragment pages first to avoid using extra memory.
  if (!fragmentVectors_.empty()) {
    return createFragmentsOutput();
  }

  if (!noMoreInput_ || finished_) {
    return nullptr;
  }

  if (aggregation_ != nullptr && !aggregation_->isFinished()) {
    const std::string commitContext = createTableCommitContext(false);
    return TableWriteTraits::createAggregationStatsOutput(
        outputType_,
        aggregation_->getOutput(),
        StringView(commitContext),
        pool());
  }
  finished_ = true;
  return createLastOutput();
}

RowVectorPtr TableWriteMerge::createFragmentsOutput() {
  BOLT_CHECK(!fragmentVectors_.empty());

  auto outputFragmentVector = fragmentVectors_.front();
  fragmentVectors_.pop();
  const int numOutputRows = outputFragmentVector->size();
  std::vector<VectorPtr> outputColumns(outputType_->size());
  for (int outputChannel = 0; outputChannel < outputType_->size();
       ++outputChannel) {
    if (outputChannel == TableWriteTraits::kFragmentChannel) {
      outputColumns[outputChannel] = std::move(outputFragmentVector);
    } else if (outputChannel == TableWriteTraits::kContextChannel) {
      const std::string commitContext = createTableCommitContext(false);
      outputColumns[outputChannel] =
          std::make_shared<ConstantVector<StringView>>(
              pool(),
              numOutputRows,
              false /*isNull*/,
              outputType_->childAt(outputChannel),
              StringView(commitContext));
    } else {
      outputColumns[outputChannel] = BaseVector::createNullConstant(
          outputType_->childAt(outputChannel), numOutputRows, pool());
    }
  }
  return std::make_shared<RowVector>(
      pool(), outputType_, nullptr, numOutputRows, outputColumns);
}

std::string TableWriteMerge::createTableCommitContext(bool lastOutput) const {
  folly::dynamic commitContext = lastCommitContext_;
  commitContext[TableWriteTraits::klastPageContextKey] = lastOutput;
  return folly::toJson(commitContext);
}

RowVectorPtr TableWriteMerge::createLastOutput() {
  BOLT_CHECK(
      lastCommitContext_[TableWriteTraits::klastPageContextKey].asBool(),
      "unexpected last table commit context: {}",
      lastCommitContext_.asString());

  auto output = BaseVector::create<RowVector>(outputType_, 1, pool());
  output->resize(1);
  for (int outputChannel = 0; outputChannel < outputType_->size();
       ++outputChannel) {
    if (outputChannel == TableWriteTraits::kRowCountChannel) {
      auto* rowCounterVector =
          output->childAt(outputChannel)->asFlatVector<int64_t>();
      rowCounterVector->resize(1);
      rowCounterVector->set(0, numRows_);
    } else if (outputChannel == TableWriteTraits::kContextChannel) {
      auto* contextVector =
          output->childAt(outputChannel)->asFlatVector<StringView>();
      contextVector->resize(1);
      const std::string lastCommitContext = createTableCommitContext(true);
      contextVector->set(0, StringView(lastCommitContext));
    } else {
      // All the fragments and statistics shall have already been outputted.
      BOLT_CHECK(fragmentVectors_.empty());
      output->childAt(outputChannel) = BaseVector::createNullConstant(
          outputType_->childAt(outputChannel), 1, pool());
    }
  }
  return output;
}

bool TableWriteMerge::isStatistics(RowVectorPtr input) {
  return input->childAt(TableWriteTraits::kRowCountChannel)->isNullAt(0) &&
      input->childAt(TableWriteTraits::kFragmentChannel)->isNullAt(0);
}
} // namespace bytedance::bolt::exec
