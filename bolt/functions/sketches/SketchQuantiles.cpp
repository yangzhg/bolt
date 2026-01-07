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

#include <DataSketches/quantiles_sketch.hpp>
#include <memory>
#include <string>

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
using QuantilesSketch = datasketches::quantiles_sketch<T>;

template <typename T>
class SketchQuantilesAccumulator {
  std::unique_ptr<QuantilesSketch<T>> sketch_;
  double rank_;
  static constexpr auto headerPrefixSize = sizeof(char);
  static constexpr auto rankIdx = headerPrefixSize;
  static constexpr auto rankSize = sizeof(rank_);
  static constexpr int headerSize = rankSize + headerPrefixSize;

 public:
  SketchQuantilesAccumulator() {
    sketch_ = nullptr;
    rank_ = 0;
  }

  bool hasInitialized() {
    return sketch_ != nullptr;
  }

  void initialize(uint32_t k, double rank) {
    sketch_ = std::make_unique<QuantilesSketch<T>>(k);
    rank_ = rank;
  }

  void initialize(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    DecodedVector decodedVector;

    decodedVector.decode(*args[2], rows, true);
    double rank = decodedVector.template valueAt<double>(0);

    decodedVector.decode(*args[1], rows, true);
    uint32_t k = decodedVector.template valueAt<uint32_t>(0);

    initialize(k, rank);
  }

  std::string serialize() {
    std::string str;
    if (hasInitialized()) {
      auto serialized = sketch_->template serialize<serde<T>>(headerSize);
      serialized[0] = headerSize;
      *(reinterpret_cast<double*>(serialized.data() + rankIdx)) = rank_;
      str = std::string(serialized.begin(), serialized.end());
    }
    return str;
  }

  static SketchQuantilesAccumulator deserialize(StringView& sv) {
    SketchQuantilesAccumulator result;
    if (sv.size() > 0) {
      auto data = sv.data();
      int serializedIdx = data[0];
      result.rank_ = *(reinterpret_cast<const double*>(data + rankIdx));
      auto sketch = QuantilesSketch<T>::template deserialize<serde<T>>(
          data + serializedIdx, sv.size() - serializedIdx);
      result.sketch_ = std::make_unique<QuantilesSketch<T>>(std::move(sketch));
    }

    return result;
  }

  void update(T t) {
    if (hasInitialized()) {
      sketch_->update(t);
    }
  }

  T getResult() {
    if (hasInitialized())
      return sketch_->template get_quantile(rank_);

    return T();
  }

  void merge(SketchQuantilesAccumulator& other) {
    if (!hasInitialized() && !other.hasInitialized())
      return;

    if (!hasInitialized()) {
      this->initialize(other.sketch_->get_k(), other.rank_);
    }

    if (other.hasInitialized()) {
      sketch_->merge(*other.sketch_);
    }
  }
};

template <typename T>
class SketchQuantilesAggregate
    : public SketchAggregateBase<T, SketchQuantilesAccumulator<T>> {
 public:
  SketchQuantilesAggregate(const TypePtr& resultType)
      : SketchAggregateBase<T, SketchQuantilesAccumulator<T>>(resultType) {}

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
        "The type of second argument for {} must be double",
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
std::unique_ptr<exec::Aggregate> createSketchQuantilesAggregate(
    const TypePtr& resultType) {
  using T = typename TypeTraits<kind>::NativeType;
  return std::make_unique<SketchQuantilesAggregate<T>>(resultType);
}

std::unique_ptr<exec::Aggregate> dispatchType(
    const TypePtr& type,
    const TypePtr& resultType) {
  switch (type->kind()) {
    case TypeKind::TINYINT:
      return createSketchQuantilesAggregate<TypeKind::TINYINT>(resultType);
    case TypeKind::SMALLINT:
      return createSketchQuantilesAggregate<TypeKind::SMALLINT>(resultType);
    case TypeKind::INTEGER:
      return createSketchQuantilesAggregate<TypeKind::INTEGER>(resultType);
    case TypeKind::BIGINT:
      return createSketchQuantilesAggregate<TypeKind::BIGINT>(resultType);
    case TypeKind::DOUBLE:
      return createSketchQuantilesAggregate<TypeKind::DOUBLE>(resultType);
    case TypeKind::REAL:
      return createSketchQuantilesAggregate<TypeKind::REAL>(resultType);
    case TypeKind::VARCHAR:
      return createSketchQuantilesAggregate<TypeKind::VARCHAR>(resultType);
    case TypeKind::TIMESTAMP:
      return createSketchQuantilesAggregate<TypeKind::TIMESTAMP>(resultType);
    default:
      return nullptr;
  }
}

bool registerSketchQuantiles(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  for (const auto& inputType : primitiveTypes) {
    if (inputType == "boolean") {
      // TODO: Add serializer of the vector<bool>. Generic serializer does not
      // work due to vector<bool>::data is not existent.
      continue;
    }

    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(inputType)
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
          return std::make_unique<SketchQuantilesAggregate<uint8_t>>(
              VARBINARY());
        }

        TypePtr type = isRawInput ? argTypes[0] : resultType;

        return dispatchType(type, resultType);
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerSketchQuantiles(kQuantiles);

} // namespace

void registerSketchQuantilesAggregate() {
  registerSketchQuantiles(kQuantiles);
}
} // namespace bytedance::bolt::aggregate
