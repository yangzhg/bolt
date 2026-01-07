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
#include <string>

#include <DataSketches/frequent_items_sketch.hpp>
#include "SerDe.hpp"
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

template <typename T>
using FISketch = datasketches::frequent_items_sketch<
    T,
    uint64_t,
    std::hash<T>,
    std::equal_to<T>,
    serde<T>>;

template <typename T>
class SketchFIAccumulator {
  const static constexpr double DEFAULT_ESTIMATE = 0.0;
  std::unique_ptr<FISketch<T>> sketch_;
  uint8_t lg_max_map_size_;
  static constexpr auto headerPrefixSize = sizeof(char);
  static constexpr auto maxMapSizeIdx = headerPrefixSize;
  static constexpr auto maxMapSizeSize = sizeof(lg_max_map_size_);
  static constexpr int headerSize = maxMapSizeSize + headerPrefixSize;

 public:
  SketchFIAccumulator() {
    sketch_ = nullptr;
    lg_max_map_size_ = 0;
  }

  void initialize(uint8_t lg_max_map_size) {
    sketch_ = std::make_unique<FISketch<T>>(lg_max_map_size);
    lg_max_map_size_ = lg_max_map_size;
  }

  void initialize(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    DecodedVector decodedVector;
    decodedVector.decode(*args[1], rows, true);
    auto lg_max_map_size = decodedVector.template valueAt<uint8_t>(0);
    initialize(lg_max_map_size);
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

    auto serialized = sketch_->serialize(headerSize);
    serialized[0] = headerSize;
    *(reinterpret_cast<uint8_t*>(serialized.data() + maxMapSizeIdx)) =
        lg_max_map_size_;
    return std::string(serialized.begin(), serialized.end());
  }

  static SketchFIAccumulator deserialize(const StringView& sv) {
    SketchFIAccumulator result;
    if (sv.empty())
      return result;

    auto data = sv.data();
    int serializedIdx = data[0];
    result.lg_max_map_size_ =
        *(reinterpret_cast<const uint8_t*>(data + maxMapSizeIdx));
    auto sketch = FISketch<T>::deserialize(
        data + serializedIdx, sv.size() - serializedIdx);
    result.sketch_ = std::make_unique<FISketch<T>>(std::move(sketch));
    return result;
  }

  T getResult() {
    if (!hasInitialized())
      return T();

    auto items = sketch_->get_frequent_items(datasketches::NO_FALSE_POSITIVES);
    if (items.empty())
      return T();

    return items[0].get_item();
  }

  void update(T t) {
    if (hasInitialized()) {
      sketch_->update(t);
    }
  }

  void merge(const SketchFIAccumulator& other) {
    if (!other.hasInitialized())
      return;

    if (!hasInitialized()) {
      initialize(other.lg_max_map_size_);
    }

    sketch_->merge(*other.sketch_);
  }
};

template <typename T>
class SketchFIAggregate
    : public SketchAggregateBase<T, SketchFIAccumulator<T>> {
 public:
  SketchFIAggregate(const TypePtr& resultType)
      : SketchAggregateBase<T, SketchFIAccumulator<T>>(resultType) {}

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BOLT_CHECK(result);
    auto vector = (*result)->as<FlatVector<T>>();

    vector->resize(numGroups);
    uint64_t* rawNulls = this->getRawNulls(vector);

    T* rawValues = vector->mutableRawValues();
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      this->clearNull(rawNulls, i);
      auto* accumulator = this->getAccumulator(group);
      rawValues[i] = accumulator->getResult();
    }
  }
};

template <TypeKind kind>
std::unique_ptr<exec::Aggregate> createSketchFIAggregate(
    const TypePtr& resultType) {
  using T = typename TypeTraits<kind>::NativeType;
  return std::make_unique<SketchFIAggregate<T>>(resultType);
}

void checkInputs(
    bool isRawInput,
    const std::string& name,
    const std::vector<TypePtr>& argTypes) {
  if (isRawInput) {
    BOLT_USER_CHECK_EQ(
        argTypes.size(), 2, "Wrong number of arguments passed to {}", name);
    BOLT_USER_CHECK_EQ(
        argTypes[1]->kind(),
        TypeKind::BIGINT,
        "The type of second argument for {} must be big int",
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

bool registerSketchFI(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  for (const auto& inputType : primitiveTypes) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(inputType)
                             .intermediateType("varbinary")
                             .argumentType(inputType)
                             .argumentType("bigint")
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
          return std::make_unique<SketchFIAggregate<uint8_t>>(VARBINARY());
        }

        TypePtr type = isRawInput ? argTypes[0] : resultType;

        return BOLT_DYNAMIC_SCALAR_SKETCH_TYPE_DISPATCH(
            createSketchFIAggregate, type->kind(), resultType);
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) = registerSketchFI(kFI);

} // namespace

void registerSketchFIAggregate() {
  registerSketchFI(kFI);
}
} // namespace bytedance::bolt::aggregate
