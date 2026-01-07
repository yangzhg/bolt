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

#include <DataSketches/theta_sketch.hpp>
#include <DataSketches/theta_union.hpp>

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

using UpdateThetaSketch = datasketches::update_theta_sketch;
using CompactThetaSketch = datasketches::compact_theta_sketch;
using ThetaSketch = datasketches::theta_sketch;
using WrappedCompactThetaSketch = datasketches::wrapped_compact_theta_sketch;

class SketchThetaAccumulator {
  const static constexpr double DEFAULT_ESTIMATE = 0.0;
  std::unique_ptr<ThetaSketch> sketch_;

 public:
  SketchThetaAccumulator() {
    sketch_ = nullptr;
  }

  void initialize(uint8_t logK, float p) {
    auto sketch = UpdateThetaSketch::builder().set_lg_k(logK).set_p(p).build();
    sketch_ = std::make_unique<UpdateThetaSketch>(std::move(sketch));
  }

  void initialize(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    DecodedVector decodedVector;

    decodedVector.decode(*args[1], rows, true);
    uint8_t logK = decodedVector.template valueAt<uint8_t>(0);

    decodedVector.decode(*args[2], rows, true);
    float p = decodedVector.template valueAt<double>(0);

    initialize(logK, p);
  }

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

    auto sketch = dynamic_cast<UpdateThetaSketch*>(sketch_.get());
    if (!sketch)
      return "";

    auto serialized = sketch->compact().serialize();
    return std::string(serialized.begin(), serialized.end());
  }

  static SketchThetaAccumulator deserialize(const StringView& sv) {
    SketchThetaAccumulator result;
    if (sv.empty())
      return result;

    auto sketch = CompactThetaSketch::deserialize(sv.data(), sv.size());
    result.sketch_ = std::make_unique<CompactThetaSketch>(std::move(sketch));
    return result;
  }

  template <typename T>
  void update(T t) {
    if (hasInitialized()) {
      auto sketch = dynamic_cast<UpdateThetaSketch*>(sketch_.get());
      if (sketch)
        sketch->update(t);
    }
  }

  void merge(const SketchThetaAccumulator& other) {
    if (!other.hasInitialized())
      return;

    auto u = datasketches::theta_union::builder().build();

    auto sketch = dynamic_cast<CompactThetaSketch*>(other.sketch_.get());
    auto serialized = sketch->serialize();
    u.update(
        WrappedCompactThetaSketch::wrap(serialized.data(), serialized.size()));

    if (hasInitialized()) {
      sketch = dynamic_cast<CompactThetaSketch*>(sketch_.get());
      serialized = sketch->serialize();
      u.update(WrappedCompactThetaSketch::wrap(
          serialized.data(), serialized.size()));
    }

    auto result = u.get_result();
    sketch_ = std::make_unique<CompactThetaSketch>(std::move(result));
  }
};

template <typename T>
class SketchThetaAggregate
    : public SketchAggregateBase<T, SketchThetaAccumulator> {
 public:
  SketchThetaAggregate(const TypePtr& resultType)
      : SketchAggregateBase<T, SketchThetaAccumulator>(resultType) {}

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

void checkInputs(
    bool isRawInput,
    const std::string& name,
    const std::vector<TypePtr>& argTypes) {
  if (isRawInput) {
    BOLT_USER_CHECK_EQ(
        argTypes.size(), 3, "Wrong number of arguments passed to {}", name);
    BOLT_USER_CHECK_EQ(
        argTypes[1]->kind(),
        TypeKind::BIGINT,
        "The type of second argument for {} must be big int",
        name);
    BOLT_USER_CHECK_EQ(
        argTypes[2]->kind(),
        TypeKind::DOUBLE,
        "The type of third argument for {} must be double",
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
}

template <TypeKind kind>
std::unique_ptr<exec::Aggregate> createSketchThetaAggregate(
    const TypePtr& resultType) {
  using T = typename TypeTraits<kind>::NativeType;
  return std::make_unique<SketchThetaAggregate<T>>(resultType);
}

bool registerSketchTheta(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  for (const auto& inputType : primitiveTypes) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("double")
                             .intermediateType("varbinary")
                             .argumentType(inputType)
                             .argumentType("bigint")
                             .argumentType("double")
                             .build());
  }

  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
        auto isRawInput = exec::isRawInput(step);

        checkInputs(isRawInput, name, argTypes);

        if (!isRawInput && exec::isPartialOutput(step)) {
          return std::make_unique<SketchThetaAggregate<uint8_t>>(VARBINARY());
        }

        TypePtr type = isRawInput ? argTypes[0] : resultType;

        return BOLT_DYNAMIC_SCALAR_SKETCH_TYPE_DISPATCH(
            createSketchThetaAggregate, type->kind(), resultType);
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerSketchTheta(kTheta);

} // namespace

void registerSketchThetaAggregate() {
  registerSketchTheta(kTheta);
}
} // namespace bytedance::bolt::aggregate
