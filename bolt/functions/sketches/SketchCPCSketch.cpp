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

#include <memory>

#include <DataSketches/cpc_sketch.hpp>
#include <DataSketches/cpc_union.hpp>

#include "SketchAggregateBase.h"
#include "SketchFunctionNames.h"
#include "bolt/common/base/RandomUtil.h"
#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/expression/FunctionSignature.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::aggregate {
namespace {

using CPCSketch = datasketches::cpc_sketch;

class SketchCPCSketchAccumulator {
  const static constexpr double DEFAULT_ESTIMATE = 0.0;
  std::unique_ptr<CPCSketch> sketch_;

 public:
  SketchCPCSketchAccumulator() {
    sketch_ = nullptr;
  }

  void initialize(uint8_t logK) {
    sketch_ = std::make_unique<CPCSketch>(logK);
  }

  void initialize(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {}

  bool hasInitialized() const {
    return sketch_ != nullptr;
  }

  double getResult() const {
    if (hasInitialized())
      return sketch_->get_estimate();

    return DEFAULT_ESTIMATE;
  }

  std::string serialize() {
    if (!hasInitialized())
      return "";
    auto serialized = sketch_->serialize();
    return std::string(serialized.begin(), serialized.end());
  }

  static SketchCPCSketchAccumulator deserialize(const StringView& sv) {
    SketchCPCSketchAccumulator result;
    if (sv.empty())
      return result;

    auto sketch = CPCSketch::deserialize(sv.data(), sv.size());
    result.sketch_ = std::make_unique<CPCSketch>(std::move(sketch));
    return result;
  }

  void update(const StringView& t) {
    if (!hasInitialized()) {
      auto sketch = CPCSketch::deserialize(t.data(), t.size());
      sketch_ = std::make_unique<CPCSketch>(std::move(sketch));
    } else {
      datasketches::cpc_union u;
      u.update(*sketch_);

      if (!t.empty()) {
        auto sketch = CPCSketch::deserialize(t.data(), t.size());
        u.update(sketch);
      }
      auto result = u.get_result();
      sketch_ = std::make_unique<CPCSketch>(std::move(result));
    }
  }

  void merge(const SketchCPCSketchAccumulator& other) {
    if (!other.hasInitialized())
      return;

    if (!hasInitialized()) {
      initialize(other.sketch_->get_lg_k());
    }

    datasketches::cpc_union u(sketch_->get_lg_k());
    u.update(*sketch_);
    u.update(*other.sketch_);
    auto result = u.get_result();
    sketch_ = std::make_unique<CPCSketch>(std::move(result));
  }
};

class SketchCPCAggregate
    : public SketchAggregateBase<StringView, SketchCPCSketchAccumulator> {
 public:
  SketchCPCAggregate(const TypePtr& resultType)
      : SketchAggregateBase<StringView, SketchCPCSketchAccumulator>(
            resultType) {}

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BOLT_CHECK(result);
    auto vector = (*result)->as<FlatVector<double>>();

    vector->resize(numGroups);
    uint64_t* rawNulls = this->getRawNulls(vector);

    double* rawValues = vector->mutableRawValues();
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      this->clearNull(rawNulls, i);
      auto* accumulator = this->getAccumulator(group);
      rawValues[i] = accumulator->getResult();
    }
  }
};

template <TypeKind kind>
std::unique_ptr<exec::Aggregate> createSketchCPCAggregate(
    const TypePtr& resultType) {
  using T = typename TypeTraits<kind>::NativeType;
  return std::make_unique<SketchCPCAggregate>(resultType);
}

bool registerCPCSketch(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("double")
                           .intermediateType("varbinary")
                           .argumentType("varbinary")
                           .build());

  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
        auto isRawInput = exec::isRawInput(step);

        if (isRawInput) {
          BOLT_USER_CHECK_EQ(
              argTypes.size(),
              1,
              "Wrong number of arguments passed to {}",
              name);
          BOLT_USER_CHECK_EQ(
              argTypes[0]->kind(),
              TypeKind::VARBINARY,
              "The type of second argument for {} must be binary",
              name);
        } else {
          BOLT_USER_CHECK_EQ(
              argTypes.size(),
              1,
              "The number of partial result for {} must be one",
              name);
          BOLT_USER_CHECK_EQ(
              argTypes[0]->kind(),
              TypeKind::VARBINARY,
              "The type of partial result for {} must be VARBINARY",
              name);
        }

        if (!isRawInput && exec::isPartialOutput(step)) {
          return std::make_unique<SketchCPCAggregate>(VARBINARY());
        }

        TypePtr type = isRawInput ? argTypes[0] : resultType;

        return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
            createSketchCPCAggregate, type->kind(), resultType);
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerCPCSketch(kCPCSketch);

} // namespace

void registerCPCSketchAggregate() {
  registerCPCSketch(kCPCSketch);
}
} // namespace bytedance::bolt::aggregate
