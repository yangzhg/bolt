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

#include <cudf/aggregation.hpp>
#include <cudf/binaryop.hpp>
#include <cudf/column/column_factories.hpp>
#include <cudf/concatenate.hpp>
#include <cudf/detail/utilities/stream_pool.hpp>
#include <cudf/reduction.hpp>
#include <cudf/sorting.hpp>
#include <cudf/unary.hpp>
#include <cudf/utilities/error.hpp>
#include <functional>
#include <iostream>
#include <iterator>
#include <numeric>
#include <unordered_map>
#include <vector>

#include "bolt/cudf/exec/HashAggregation.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/AggregateInfo.h"

namespace bolt::cudf::exec {

Aggregator::Aggregator(
    rmm::cuda_stream_view stream,
    bytedance::bolt::core::AggregationNode::Step step,
    const bytedance::bolt::TypePtr& outputType)
    : stream_(stream),
      step_(step),
      outputType_(outputType),
      cudfOutputType_(::cudf::data_type(fromBoltType(outputType))) {}

std::vector<std::unique_ptr<::cudf::column>> Aggregator::simpleFinalize(
    std::vector<std::unique_ptr<::cudf::column>>&& intermediateCols) {
  if (!intermediateCols.empty() &&
      intermediateCols[0]->type() != cudfOutputType_) {
    intermediateCols[0] =
        ::cudf::cast(*intermediateCols[0], cudfOutputType_, stream_);
  }
  return std::move(intermediateCols);
}

template <typename CudfAggregation>
class SimpleAggregator : public Aggregator {
 public:
  using Aggregator::Aggregator;

  void addGroupbyRequests(
      ::cudf::column_view values,
      std::vector<::cudf::groupby::aggregation_request>& requests) override {
    ::cudf::groupby::aggregation_request req;
    req.values = std::move(values);
    req.aggregations.emplace_back(
        CudfAggregation{}.template operator()<::cudf::groupby_aggregation>());
    requests.emplace_back(std::move(req));
  }

  std::vector<std::unique_ptr<::cudf::column>> reduce(
      const ::cudf::column_view& values) override {
    std::unique_ptr<::cudf::scalar> outputScalar;
    try {
      outputScalar = ::cudf::reduce(
          values,
          *(CudfAggregation{}
                .template operator()<::cudf::reduce_aggregation>()),
          values.type(),
          stream_);
    } catch (const ::cudf::logic_error& e) {
      BOLT_FAIL("Reduce aggregation failed: {}", e.what());
    }
    std::vector<std::unique_ptr<::cudf::column>> result;
    result.emplace_back(
        ::cudf::make_column_from_scalar(*outputScalar, 1, stream_));
    return result;
  }

  std::vector<std::unique_ptr<::cudf::column>> finalize(
      std::vector<std::unique_ptr<::cudf::column>>&& intermediateCols)
      override {
    return simpleFinalize(std::move(intermediateCols));
  }

  int intermediateColumnCount() const override {
    return 1;
  }
};

class CountAggregator : public Aggregator {
 public:
  using Aggregator::Aggregator;

  void addGroupbyRequests(
      ::cudf::column_view values,
      std::vector<::cudf::groupby::aggregation_request>& requests) override {
    ::cudf::groupby::aggregation_request req;
    req.values = std::move(values);
    if (isStepWithRawInput()) {
      req.aggregations.emplace_back(
          ::cudf::make_count_aggregation<::cudf::groupby_aggregation>());
    } else {
      req.aggregations.emplace_back(
          ::cudf::make_sum_aggregation<::cudf::groupby_aggregation>());
    }
    requests.emplace_back(std::move(req));
  }

  std::vector<std::unique_ptr<::cudf::column>> reduce(
      const ::cudf::column_view& values) override {
    std::unique_ptr<::cudf::scalar> outputScalar;
    if (isStepWithRawInput()) {
      // TODO: A mode to include null values in count aggregation.
      outputScalar = std::make_unique<::cudf::numeric_scalar<int64_t>>(
          values.size() - values.null_count(), true, stream_);
    } else {
      try {
        outputScalar = ::cudf::reduce(
            values,
            *::cudf::make_sum_aggregation<::cudf::reduce_aggregation>(),
            values.type(),
            stream_);
      } catch (const ::cudf::logic_error& e) {
        BOLT_FAIL("Count aggregation failed: {}", e.what());
      }
    }
    std::vector<std::unique_ptr<::cudf::column>> result;
    result.emplace_back(
        ::cudf::make_column_from_scalar(*outputScalar, 1, stream_));
    return result;
  }

  std::vector<std::unique_ptr<::cudf::column>> finalize(
      std::vector<std::unique_ptr<::cudf::column>>&& intermediateCols)
      override {
    return simpleFinalize(std::move(intermediateCols));
  }

  int intermediateColumnCount() const override {
    return 1;
  }
};

class AvgAggregator : public Aggregator {
 public:
  using Aggregator::Aggregator;

  void addGroupbyRequests(
      ::cudf::column_view values,
      std::vector<::cudf::groupby::aggregation_request>& requests) override {
    ::cudf::groupby::aggregation_request req;
    req.values = std::move(values);
    if (isStepWithRawInput()) {
      req.aggregations.emplace_back(
          ::cudf::make_sum_aggregation<::cudf::groupby_aggregation>());
      req.aggregations.emplace_back(
          ::cudf::make_count_aggregation<::cudf::groupby_aggregation>());
    } else {
      req.aggregations.emplace_back(
          ::cudf::make_sum_aggregation<::cudf::groupby_aggregation>());
    }
    requests.emplace_back(std::move(req));
  }

  std::vector<std::unique_ptr<::cudf::column>> reduce(
      const ::cudf::column_view& values) override {
    std::vector<std::unique_ptr<::cudf::column>> result;
    if (isStepWithRawInput()) {
      try {
        auto sumScalar = ::cudf::reduce(
            values,
            *::cudf::make_sum_aggregation<::cudf::reduce_aggregation>(),
            values.type(),
            stream_);
        result.emplace_back(
            ::cudf::make_column_from_scalar(*sumScalar, 1, stream_));
      } catch (const ::cudf::logic_error& e) {
        BOLT_FAIL("Avg aggregation failed: {}", e.what());
      }
      auto countScalar = std::make_unique<::cudf::numeric_scalar<int64_t>>(
          values.size() - values.null_count(), true, stream_);
      result.emplace_back(
          ::cudf::make_column_from_scalar(*countScalar, 1, stream_));
    } else {
      try {
        auto sumScalar = ::cudf::reduce(
            values,
            *::cudf::make_sum_aggregation<::cudf::reduce_aggregation>(),
            values.type(),
            stream_);
        result.emplace_back(
            ::cudf::make_column_from_scalar(*sumScalar, 1, stream_));
      } catch (const ::cudf::logic_error& e) {
        BOLT_FAIL("Avg aggregation failed: {}", e.what());
      }
    }
    return result;
  }

  std::vector<std::unique_ptr<::cudf::column>> finalize(
      std::vector<std::unique_ptr<::cudf::column>>&& intermediateCols)
      override {
    BOLT_CHECK_EQ(intermediateCols.size(), 2);

    auto& sumCol = intermediateCols[0];
    auto& countCol = intermediateCols[1];

    std::vector<std::unique_ptr<::cudf::column>> finalResult;
    if (isStepWithPartialOutput()) {
      std::vector<::cudf::data_type> outputTypes = {
          ::cudf::data_type(fromBoltType(outputType_->childAt(0))),
          ::cudf::data_type(fromBoltType(outputType_->childAt(1)))};

      intermediateCols[0] =
          ::cudf::cast(*intermediateCols[0], outputTypes[0], stream_);
      intermediateCols[1] =
          ::cudf::cast(*intermediateCols[1], outputTypes[1], stream_);

      finalResult.emplace_back(std::make_unique<::cudf::column>(
          ::cudf::data_type(::cudf::type_id::STRUCT),
          intermediateCols[0]->size(),
          ::rmm::device_buffer{},
          ::rmm::device_buffer{},
          0,
          std::move(intermediateCols)));
      return finalResult;
    }

    auto castedSum = ::cudf::cast(sumCol->view(), cudfOutputType_, stream_);
    auto castedCount = ::cudf::cast(countCol->view(), cudfOutputType_, stream_);

    auto avgCol = ::cudf::binary_operation(
        castedSum->view(),
        castedCount->view(),
        ::cudf::binary_operator::DIV,
        cudfOutputType_,
        stream_);
    finalResult.emplace_back(std::move(avgCol));
    return finalResult;
  }

  int intermediateColumnCount() const override {
    return 2;
  }
};

namespace {

template <typename T>
struct is_cudf_aggregation_type : std::false_type {};
template <>
struct is_cudf_aggregation_type<::cudf::groupby_aggregation> : std::true_type {
};
template <>
struct is_cudf_aggregation_type<::cudf::reduce_aggregation> : std::true_type {};
template <typename T>
inline constexpr bool is_cudf_aggregation_type_v =
    is_cudf_aggregation_type<T>::value;

struct CudfMinAggregation {
  template <typename T>
  auto operator()() const {
    static_assert(
        is_cudf_aggregation_type_v<T>,
        "Aggregation type T must be a valid cuDF aggregation type");

    return ::cudf::make_min_aggregation<T>();
  }
};

struct CudfMaxAggregation {
  template <typename T>
  auto operator()() const {
    static_assert(
        is_cudf_aggregation_type_v<T>,
        "Aggregation type T must be a valid cuDF aggregation type");

    return ::cudf::make_max_aggregation<T>();
  }
};

struct CudfSumAggregation {
  template <typename T>
  auto operator()() const {
    static_assert(
        is_cudf_aggregation_type_v<T>,
        "Aggregation type T must be a valid cuDF aggregation type");

    return ::cudf::make_sum_aggregation<T>();
  }
};

using AggregatorFactory = std::function<std::unique_ptr<Aggregator>(
    rmm::cuda_stream_view,
    bytedance::bolt::core::AggregationNode::Step,
    const bytedance::bolt::TypePtr&)>;

const std::unordered_map<std::string, AggregatorFactory> kAggregators = {
    {"sum",
     [](auto s, auto step, auto& type) {
       return std::make_unique<SimpleAggregator<CudfSumAggregation>>(
           s, step, type);
     }},
    {"min",
     [](auto s, auto step, auto& type) {
       return std::make_unique<SimpleAggregator<CudfMinAggregation>>(
           s, step, type);
     }},
    {"max",
     [](auto s, auto step, auto& type) {
       return std::make_unique<SimpleAggregator<CudfMaxAggregation>>(
           s, step, type);
     }},
    {"count",
     [](auto s, auto step, auto& type) {
       return std::make_unique<CountAggregator>(s, step, type);
     }},
    {"avg",
     [](auto s, auto step, auto& type) {
       return std::make_unique<AvgAggregator>(s, step, type);
     }},
};
} // namespace

bool HashAggregation::isSupported(
    const std::shared_ptr<const bytedance::bolt::core::AggregationNode>&
        aggregationNode) {
  for (const auto& aggregate : aggregationNode->aggregates()) {
    if (kAggregators.find(aggregate.call->name()) == kAggregators.end()) {
      return false;
    }
  }
  return true;
}

HashAggregation::HashAggregation(
    int32_t operatorId,
    bytedance::bolt::exec::DriverCtx* driverCtx,
    const std::shared_ptr<const bytedance::bolt::core::AggregationNode>&
        aggregationNode)
    : bytedance::bolt::exec::Operator(
          driverCtx,
          aggregationNode->outputType(),
          operatorId,
          aggregationNode->id(),
          aggregationNode->step() ==
                  bytedance::bolt::core::AggregationNode::Step::kPartial
              ? "cudf::PartialAggregation"
              : "cudf::Aggregation"),
      aggregationNode_(aggregationNode),
      isGlobal_(aggregationNode->groupingKeys().empty()),
      step_(aggregationNode->step()),
      totalRows_(0) {
  BOLT_CHECK_NOT_NULL(aggregationNode, "aggregationNode cannot be nullptr");
  if (::cudf::is_ptds_enabled()) {
    stream_ = ::cudf::get_default_stream();
  } else {
    // NOTE: The number of streams in the global pool is 32.
    stream_ = ::cudf::detail::global_cuda_stream_pool().get_stream();
  }
}

void HashAggregation::initialize() {
  bytedance::bolt::exec::Operator::initialize();

  std::shared_ptr<bytedance::bolt::core::ExpressionEvaluator>
      expressionEvaluator;
  auto aggregateInfos = toAggregateInfo(
      *aggregationNode_,
      *operatorCtx_,
      aggregationNode_->groupingKeys().size(),
      expressionEvaluator);

  BOLT_CHECK_EQ(aggregateInfos.size(), aggregationNode_->aggregates().size());

  bytedance::bolt::column_index_t constChannel = 0;

  for (size_t i = 0; i < aggregateInfos.size(); ++i) {
    const auto& info = aggregateInfos[i];
    const auto& aggNode = aggregationNode_->aggregates()[i];
    const auto& aggName = aggNode.call->name();

    auto factoryIt = kAggregators.find(aggName);
    BOLT_CHECK(factoryIt != kAggregators.end());
    auto aggregator = factoryIt->second(
        stream_, step_, aggregationNode_->outputType()->childAt(info.output));

    for (size_t j = 0; j < info.inputs.size(); ++j) {
      if (info.constantInputs[j] != nullptr) {
        aggregator->addInputChannel(constChannel++, info.constantInputs[j]);
      } else {
        auto channel = info.inputs[j];
        aggregator->addInputChannel(channel, nullptr);
      }
    }

    aggregators_.emplace_back(std::move(aggregator));
  }

  for (const auto& key : aggregationNode_->groupingKeys()) {
    auto channel = bytedance::bolt::exec::exprToChannel(
        key.get(), aggregationNode_->sources()[0]->outputType());
    groupingKeyIndices_.emplace_back(channel);
  }
}

void HashAggregation::addInput(bytedance::bolt::RowVectorPtr input) {
  if (input->size() == 0) {
    return;
  }
  totalRows_ += input->size();

  inputs_.emplace_back(
      std::move(toCudfTable(input, input->pool(), stream_, &interOpStats_)));
}

void HashAggregation::noMoreInput() {
  bytedance::bolt::exec::Operator::noMoreInput();
  if (inputs_.size() == 0) {
    finished_ = true;
  }
}

void HashAggregation::createCudfConstantColumn(
    const AggregatorInputChannel& inputChannel) {
  const bool shouldMaterializeConstantInput =
      cudfConstantInputs_.find(inputChannel.idx) == cudfConstantInputs_.end();
  if (shouldMaterializeConstantInput) {
    auto scalar = toCudfScalar(*inputChannel.constantInput, pool(), stream_);
    cudfConstantInputs_[inputChannel.idx] =
        ::cudf::make_column_from_scalar(*scalar, totalRows_, stream_);
  }
}

namespace {
void reduceRecursive(
    const ::cudf::column_view& column,
    Aggregator& aggregator,
    std::vector<std::unique_ptr<::cudf::column>>& intermediateCols) {
  if (column.num_children() > 0) {
    for (auto i = 0; i < column.num_children(); ++i) {
      reduceRecursive(column.child(i), aggregator, intermediateCols);
    }
  } else {
    auto reduced = aggregator.reduce(column);
    intermediateCols.insert(
        intermediateCols.end(),
        std::make_move_iterator(reduced.begin()),
        std::make_move_iterator(reduced.end()));
  }
}
} // namespace

void HashAggregation::runGlobalAggregation() {
  std::vector<std::unique_ptr<::cudf::column>> finalCols;
  for (size_t i = 0; i < aggregators_.size(); ++i) {
    auto& aggregator = aggregators_[i];
    std::vector<std::unique_ptr<::cudf::column>> intermediateCols;
    for (const auto& inputChannel : aggregator->inputChannels()) {
      if (inputChannel.isConstantInput()) {
        createCudfConstantColumn(inputChannel);
        reduceRecursive(
            cudfConstantInputs_.at(inputChannel.idx)->view(),
            *aggregator,
            intermediateCols);
      } else {
        reduceRecursive(
            inputTable_->view().column(inputChannel.idx),
            *aggregator,
            intermediateCols);
      }
    }

    auto finalized = aggregator->finalize(std::move(intermediateCols));
    finalCols.insert(
        finalCols.end(),
        std::make_move_iterator(finalized.begin()),
        std::make_move_iterator(finalized.end()));
  }
  outputTable_ = std::make_unique<::cudf::table>(std::move(finalCols));
}

namespace {
void addAggregationRequestsRecursive(
    ::cudf::column_view const& column,
    Aggregator& aggregator,
    std::vector<::cudf::groupby::aggregation_request>& requests) {
  if (column.num_children() > 0) {
    for (auto i = 0; i < column.num_children(); ++i) {
      addAggregationRequestsRecursive(column.child(i), aggregator, requests);
    }
  } else {
    aggregator.addGroupbyRequests(column, requests);
  }
}
} // namespace

void HashAggregation::runGroupbyAggregation() {
  if (totalRows_ == 0) {
    outputTable_ = std::make_unique<::cudf::table>();
    return;
  }

  std::vector<::cudf::column_view> groupingKeyColumns;
  for (auto keyIndex : groupingKeyIndices_) {
    groupingKeyColumns.emplace_back(inputTable_->view().column(keyIndex));
  }
  ::cudf::null_policy nullHandling = aggregationNode_->ignoreNullKeys()
      ? ::cudf::null_policy::EXCLUDE
      : ::cudf::null_policy::INCLUDE;
  ::cudf::groupby::groupby groupbyObj{
      ::cudf::table_view(groupingKeyColumns), nullHandling};
  std::vector<::cudf::groupby::aggregation_request> requests;
  for (size_t i = 0; i < aggregators_.size(); ++i) {
    auto& aggregator = aggregators_[i];
    for (const auto& inputChannel : aggregator->inputChannels()) {
      if (inputChannel.isConstantInput()) {
        createCudfConstantColumn(inputChannel);
        addAggregationRequestsRecursive(
            cudfConstantInputs_.at(inputChannel.idx)->view(),
            *aggregator,
            requests);
      } else {
        addAggregationRequestsRecursive(
            inputTable_->view().column(inputChannel.idx),
            *aggregator,
            requests);
      }
    }
  }

  std::pair<
      std::unique_ptr<::cudf::table>,
      std::vector<::cudf::groupby::aggregation_result>>
      result;
  try {
    result = groupbyObj.aggregate(requests);
  } catch (const ::cudf::logic_error& e) {
    BOLT_FAIL("HashAggregation failed: {}", e.what());
  }

  auto intermediateCols = result.first->release();
  size_t numAggResultCols = 0;
  for (const auto& aggResult : result.second) {
    numAggResultCols += aggResult.results.size();
  }
  intermediateCols.reserve(intermediateCols.size() + numAggResultCols);
  for (auto& aggResult : result.second) {
    for (auto& col : aggResult.results) {
      intermediateCols.emplace_back(std::move(col));
    }
  }

  std::vector<std::unique_ptr<::cudf::column>> finalCols;
  for (size_t i = 0; i < groupingKeyColumns.size(); ++i) {
    finalCols.emplace_back(std::move(intermediateCols[i]));
  }
  auto intermediateIt = intermediateCols.begin() + groupingKeyColumns.size();
  for (auto& aggregator : aggregators_) {
    const int count = aggregator->intermediateColumnCount();
    auto nextIt = intermediateIt + count;
    auto finalized =
        aggregator->finalize(std::vector<std::unique_ptr<::cudf::column>>(
            std::make_move_iterator(intermediateIt),
            std::make_move_iterator(nextIt)));
    intermediateIt = nextIt;

    finalCols.insert(
        finalCols.end(),
        std::make_move_iterator(finalized.begin()),
        std::make_move_iterator(finalized.end()));
  }
  outputTable_ = std::make_unique<::cudf::table>(std::move(finalCols));
}

bytedance::bolt::RowVectorPtr HashAggregation::getOutput() {
  if (finished_ || !noMoreInput_) {
    return nullptr;
  }

  if (inputs_.size() > 0) {
    std::vector<::cudf::table_view> inputViews;
    for (auto& input : inputs_) {
      inputViews.emplace_back(input->view());
    }
    try {
      inputTable_ = ::cudf::concatenate(inputViews, stream_);
    } catch (const std::exception& e) {
      BOLT_FAIL("HashAggregation failed: {}", e.what());
    }
  } else {
    BOLT_CHECK_EQ(inputs_.size(), 1, "HashAggregation expects 1 input");
    inputTable_ = std::move(inputs_[0]);
  }

  finished_ = true;

  if (isGlobal_) {
    runGlobalAggregation();
  } else {
    runGroupbyAggregation();
  }
  stream_.synchronize();

  bytedance::bolt::RowVectorPtr outputVector = nullptr;
  if (outputTable_->num_rows() != 0 && outputTable_->num_columns() != 0) {
    outputVector = toBoltRowVector(outputTable_->view(), pool(), stream_);
    outputVector->setType(aggregationNode_->outputType());
  }

  recordInterOpStats(*this, interOpStats_);

  return outputVector;
}

} // namespace bolt::cudf::exec
