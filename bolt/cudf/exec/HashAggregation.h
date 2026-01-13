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

#include <cudf/groupby.hpp>
#include <cudf/table/table_view.hpp>
#include <cudf/types.hpp>
#include <rmm/cuda_stream_view.hpp>
#include <optional>
#include <set>
#include <vector>

#include "bolt/core/PlanNode.h"
#include "bolt/cudf/exec/InterOp.h"
#include "bolt/exec/Operator.h"

namespace bolt::cudf::exec {

struct AggregatorInputChannel {
  bytedance::bolt::column_index_t idx;
  bytedance::bolt::VectorPtr constantInput;

  inline bool isConstantInput() const {
    return constantInput != nullptr;
  }
};

class Aggregator {
 public:
  Aggregator(
      rmm::cuda_stream_view stream,
      bytedance::bolt::core::AggregationNode::Step step,
      const bytedance::bolt::TypePtr& outputType);
  virtual ~Aggregator() = default;

  virtual void addGroupbyRequests(
      ::cudf::column_view values,
      std::vector<::cudf::groupby::aggregation_request>& requests) = 0;

  virtual std::vector<std::unique_ptr<::cudf::column>> reduce(
      const ::cudf::column_view& values) = 0;

  virtual std::vector<std::unique_ptr<::cudf::column>> finalize(
      std::vector<std::unique_ptr<::cudf::column>>&& intermediateCols) = 0;

  virtual int intermediateColumnCount() const = 0;

  void addInputChannel(
      bytedance::bolt::column_index_t idx,
      bytedance::bolt::VectorPtr constantInput) {
    inputChannels_.emplace_back(AggregatorInputChannel{idx, constantInput});
  }

  const std::vector<AggregatorInputChannel>& inputChannels() const {
    return inputChannels_;
  }

 protected:
  bool isStepWithRawInput() {
    return step_ == bytedance::bolt::core::AggregationNode::Step::kSingle ||
        step_ == bytedance::bolt::core::AggregationNode::Step::kPartial;
  }

  bool isStepWithPartialOutput() {
    return step_ == bytedance::bolt::core::AggregationNode::Step::kPartial ||
        step_ == bytedance::bolt::core::AggregationNode::Step::kIntermediate;
  }

  std::vector<std::unique_ptr<::cudf::column>> simpleFinalize(
      std::vector<std::unique_ptr<::cudf::column>>&& intermediateCols);

  rmm::cuda_stream_view stream_;
  bytedance::bolt::core::AggregationNode::Step step_;
  bytedance::bolt::TypePtr outputType_;
  ::cudf::data_type cudfOutputType_;
  std::vector<AggregatorInputChannel> inputChannels_;
};

class HashAggregation : public bytedance::bolt::exec::Operator {
 public:
  static bool isSupported(
      const std::shared_ptr<const bytedance::bolt::core::AggregationNode>&
          aggregationNode);

  HashAggregation(
      int32_t operatorId,
      bytedance::bolt::exec::DriverCtx* driverCtx,
      const std::shared_ptr<const bytedance::bolt::core::AggregationNode>&
          aggregationNode);

  void initialize() override;

  bool needsInput() const override {
    return !noMoreInput_;
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
  void createCudfConstantColumn(const AggregatorInputChannel& inputChannel);

  void runGlobalAggregation();
  void runGroupbyAggregation();

  std::shared_ptr<const bytedance::bolt::core::AggregationNode>
      aggregationNode_;
  const bool isGlobal_;
  const bytedance::bolt::core::AggregationNode::Step step_;
  bool finished_ = false;

  std::unordered_map<
      bytedance::bolt::column_index_t,
      std::unique_ptr<::cudf::column>>
      cudfConstantInputs_;

  std::vector<std::unique_ptr<Aggregator>> aggregators_;

  std::vector<bytedance::bolt::column_index_t> groupingKeyIndices_;

  size_t totalRows_;
  std::vector<std::unique_ptr<::cudf::table>> inputs_;
  std::unique_ptr<::cudf::table> inputTable_;
  std::unique_ptr<::cudf::table> outputTable_;
  rmm::cuda_stream_view stream_;

  InterOpStats interOpStats_;
};

} // namespace bolt::cudf::exec
