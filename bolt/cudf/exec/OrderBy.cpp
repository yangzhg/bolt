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

#include <cudf/concatenate.hpp>
#include <cudf/copying.hpp>
#include <cudf/detail/utilities/stream_pool.hpp>
#include <cudf/sorting.hpp>
#include <exception>

#include "bolt/cudf/exec/InterOp.h"
#include "bolt/cudf/exec/OrderBy.h"

namespace bolt::cudf::exec {

OrderBy::OrderBy(
    int32_t operatorId,
    bytedance::bolt::exec::DriverCtx* driverCtx,
    const std::shared_ptr<const bytedance::bolt::core::OrderByNode>&
        orderByNode)
    : bytedance::bolt::exec::Operator(
          driverCtx,
          orderByNode->outputType(),
          operatorId,
          orderByNode->id(),
          "cudf::OrderBy"),
      orderByNode_(orderByNode) {
  // Check if orderByNode is nullptr
  BOLT_CHECK_NOT_NULL(orderByNode, "orderByNode cannot be nullptr");

  sortColumnIndices_.reserve(orderByNode->sortingKeys().size());
  sortColumnOrder_.reserve(orderByNode->sortingKeys().size());
  sortNullOrder_.reserve(orderByNode->sortingKeys().size());
  for (size_t i = 0; i < orderByNode->sortingKeys().size(); ++i) {
    const auto channel = bytedance::bolt::exec::exprToChannel(
        orderByNode->sortingKeys()[i].get(), outputType_);
    BOLT_CHECK(
        channel != bytedance::bolt::kConstantChannel,
        "OrderBy doesn't allow constant sorting keys");
    sortColumnIndices_.push_back(channel);
    auto const& sortingOrder = orderByNode->sortingOrders()[i];
    sortColumnOrder_.push_back(
        sortingOrder.isAscending() ? ::cudf::order::ASCENDING
                                   : ::cudf::order::DESCENDING);
    sortNullOrder_.push_back(
        (sortingOrder.isNullsFirst() ^ !sortingOrder.isAscending())
            ? ::cudf::null_order::BEFORE
            : ::cudf::null_order::AFTER);
  }
  if (::cudf::is_ptds_enabled()) {
    stream_ = ::cudf::get_default_stream();
  } else {
    // NOTE: The number of streams in the global pool is 32.
    stream_ = ::cudf::detail::global_cuda_stream_pool().get_stream();
  }
}

void OrderBy::addInput(bytedance::bolt::RowVectorPtr input) {
  if (input->size() == 0) {
    return;
  }

  inputs_.emplace_back(
      std::move(toCudfTable(input, input->pool(), stream_, &interOpStats_)));
}

void OrderBy::noMoreInput() {
  bytedance::bolt::exec::Operator::noMoreInput();

  if (inputs_.size() == 0) {
    finished_ = true;
    return;
  }

  if (inputs_.size() > 0) {
    std::vector<::cudf::table_view> inputViews;
    for (auto& input : inputs_) {
      inputViews.emplace_back(input->view());
    }
    try {
      inputTable_ = ::cudf::concatenate(inputViews, stream_);
    } catch (const std::exception& e) {
      BOLT_FAIL("OrderBy failed: {}", e.what());
    }
  } else {
    BOLT_CHECK_EQ(inputs_.size(), 1);
    inputTable_ = std::move(inputs_[0]);
  }

  auto keys = inputTable_->view().select(sortColumnIndices_);
  auto values = inputTable_->view();
  try {
    outputTable_ = ::cudf::sort_by_key(
        values, keys, sortColumnOrder_, sortNullOrder_, stream_);
  } catch (const ::cudf::logic_error& e) {
    BOLT_FAIL("OrderBy failed: {}", e.what());
  }
}

bytedance::bolt::RowVectorPtr OrderBy::getOutput() {
  if (finished_ || !noMoreInput_) {
    return nullptr;
  }
  finished_ = true;
  bytedance::bolt::RowVectorPtr output = nullptr;
  if (outputTable_) {
    stream_.synchronize();
    output = toBoltRowVector(outputTable_->view(), pool(), stream_);
    output->setType(outputType_);
  }

  recordInterOpStats(*this, interOpStats_);
  return output;
}

} // namespace bolt::cudf::exec
