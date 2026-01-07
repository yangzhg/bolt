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

#include <cudf/table/table.hpp>
#include <cudf/types.hpp>
#include <rmm/cuda_stream_view.hpp>

#include "bolt/exec/Operator.h"
#include "bolt/vector/ComplexVector.h"

namespace bolt::cudf::exec {

class OrderBy : public bytedance::bolt::exec::Operator {
 public:
  OrderBy(
      int32_t operatorId,
      bytedance::bolt::exec::DriverCtx* driverCtx,
      const std::shared_ptr<const bytedance::bolt::core::OrderByNode>&
          orderByNode);

  bool needsInput() const override {
    return !finished_;
  }

  void addInput(bytedance::bolt::RowVectorPtr input) override;

  void noMoreInput() override;

  bytedance::bolt::RowVectorPtr getOutput() override;

  bytedance::bolt::exec::BlockingReason isBlocked(
      bytedance::bolt::ContinueFuture* /*future*/) override {
    return bytedance::bolt::exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

 private:
  std::shared_ptr<const bytedance::bolt::core::OrderByNode> orderByNode_;
  rmm::cuda_stream_view stream_;
  std::vector<std::unique_ptr<::cudf::table>> inputs_;
  std::unique_ptr<::cudf::table> inputTable_;
  std::unique_ptr<::cudf::table> outputTable_;
  std::vector<::cudf::size_type> sortColumnIndices_;
  std::vector<::cudf::order> sortColumnOrder_;
  std::vector<::cudf::null_order> sortNullOrder_;
  bool finished_ = false;
  InterOpStats interOpStats_;
};

} // namespace bolt::cudf::exec
