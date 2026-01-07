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

#include "bolt/functions/colocate/client/ColocateAggregate.h"
#include <arrow/api.h>
#include <boost/beast/core/detail/base64.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <string>
#include <utility>
#include "bolt/exec/Aggregate.h"
#include "bolt/functions/colocate/client/ColocateUtilities.h"
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt;
namespace bytedance::bolt::functions {

static boost::uuids::random_generator generator;

class ColocateAggregate : public Aggregate {
 public:
  explicit ColocateAggregate(
      const std::string& functionName,
      const std::vector<TypePtr>& argTypes,
      TypePtr resultType,
      ColocateFunctionMetadata& metadata,
      int32_t accumulatorSize)
      : Aggregate(std::move(resultType)),
        metadata_(metadata),
        accumulatorSize_(accumulatorSize),
        call_uuid_(boost::uuids::to_string(generator())) {
    fullFunctionName_ = to_signature(functionName, argTypes);
  }

  [[nodiscard]] int32_t accumulatorFixedWidthSize() const override {
    return accumulatorSize_;
  }

  // Make it's possible to have non fixed size
  [[nodiscard]] bool isFixedSize() const override {
    return false;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    auto arrowVector = groupsToArrowVector(groups, indices);

    // Call UDFClient
    std::vector<std::string> path = {
        "udaf", fullFunctionName_, call_uuid_, "initializeNewGroups"};
    auto _ = retryCallServer(metadata_.manager, path, arrowVector, 3, 10s);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    addRawInputColocate(groups, rows, args, "addIntermediateResults");
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    addRawInputColocate(groups, rows, args, "addRawInput");
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addRawInput(&group, rows, args, mayPushdown);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addRawInput(&group, rows, args, mayPushdown);
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto vec = sizeToVector(numGroups);
    folly::Range<const vector_size_t*> indices(
        vec.data(), vec.data() + vec.size());
    auto arrowVector = groupsToArrowVector(groups, indices);
    std::vector<std::string> path = {
        "udaf", fullFunctionName_, call_uuid_, "extractAccumulators", "true"};
    auto _ = retryCallServer(metadata_.manager, path, arrowVector, 3, 10s);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto vec = sizeToVector(numGroups);
    folly::Range<const vector_size_t*> indices(
        vec.data(), vec.data() + vec.size());
    auto arrowVector = groupsToArrowVector(groups, indices);
    std::vector<std::string> path = {
        "udaf", fullFunctionName_, call_uuid_, "extractValues"};
    auto functionCalledResult =
        retryCallServer(metadata_.manager, path, arrowVector, 3, 10s);
    arrowVectorsToBolt(functionCalledResult, result, allocator_->pool());
  }

 private:
  void addRawInputColocate(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      const std::string& callName) {
    // Decode all vectors
    std::vector<VectorPtr> groupVectors;
    groupVectors.push_back(groupsToVector(groups, rows));
    for (auto& arg : args) {
      auto decoded = decodeSelectedVector(rows, arg);
      groupVectors.push_back(decoded);
    }

    auto arrowVector = boltToArrowVector(groupVectors, allocator_->pool());
    std::vector<std::string> path = {
        "udaf", fullFunctionName_, call_uuid_, callName, "true"};
    auto _ = retryCallServer(metadata_.manager, path, arrowVector, 3, 10s);
  }

  static std::vector<vector_size_t> sizeToVector(size_t size) {
    std::vector<vector_size_t> vec;
    vec.reserve(size);
    for (vector_size_t j = 0; j < size; ++j) {
      vec.push_back(j);
    }
    return vec;
  }

  // Bolt's group is not really embedded with meaningful content, the group's
  // pointer can be used to differentiated
  static StringView base64GroupPointer(const char* group) {
    auto pv = reinterpret_cast<uintptr_t>(group);
    unsigned char bytes[sizeof(uintptr_t)];
    std::memcpy(bytes, &pv, sizeof(uintptr_t));
    std::string encoded;
    encoded.resize(
        boost::beast::detail::base64::encoded_size(sizeof(uintptr_t)));
    boost::beast::detail::base64::encode(&encoded[0], bytes, sizeof(uintptr_t));
    return StringView(encoded);
  }

  VectorPtr groupsToVector(char** groups, const SelectivityVector& rows) {
    auto groupFlatVector =
        std::dynamic_pointer_cast<FlatVector<StringView>>(BaseVector::create(
            VARCHAR(),
            (vector_size_t)rows.countSelected(),
            allocator_->pool()));
    auto index = 0;
    rows.applyToSelected([&](auto row) {
      auto group = groups[row];
      groupFlatVector->set(index++, base64GroupPointer(group));
    });
    return groupFlatVector;
  }

  std::shared_ptr<RecordBatch> groupsToArrowVector(
      char** groups,
      folly::Range<const vector_size_t*> indices) const {
    // Create bolt vector for groups
    auto groupFlatVector =
        std::dynamic_pointer_cast<FlatVector<StringView>>(BaseVector::create(
            VARCHAR(), (vector_size_t)indices.size(), allocator_->pool()));
    for (auto i = 0; i < indices.size(); ++i) {
      auto index = indices[i];
      auto group = groups[index];
      groupFlatVector->set(i, base64GroupPointer(group));
    }
    std::vector<VectorPtr> rowVector{groupFlatVector};
    return boltToArrowVector(rowVector, allocator_->pool());
  }

  std::string fullFunctionName_;

  ColocateFunctionMetadata& metadata_;

  const int32_t accumulatorSize_;

  const std::string call_uuid_;
};

void registerColocateAggregate(
    const std::string& name,
    std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>& signatures,
    ColocateFunctionMetadata& metadata,
    bool registerCompanionFunctions,
    bool overwrite) {
  registerAggregateFunction(
      name,
      signatures,
      [name, &metadata](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
        BOLT_CHECK_GE(
            argTypes.size(), 1, "{} takes at most one argument", name);
        return std::make_unique<ColocateAggregate>(
            name, argTypes, resultType, metadata, 32);
      },
      registerCompanionFunctions,
      overwrite);
}
} // namespace bytedance::bolt::functions
