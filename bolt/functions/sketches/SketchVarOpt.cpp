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

#include <DataSketches/var_opt_sketch.hpp>
#include <DataSketches/var_opt_union.hpp>
#include <memory>
#include <string>
#include <type_traits>

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

/*TODO: Cleanup serialization and deserializtion of types*/
template <typename T>
std::string to_string(T t) {
  if constexpr (std::is_arithmetic<T>::value) {
    return std::to_string(t);
  }
  return "";
}

template <>
std::string to_string<uint8_t>(uint8_t t) {
  return std::to_string(t);
}

template <typename T>
T ston(std::string str) {
  return T();
}

template <>
int ston<int>(std::string str) {
  return std::stoi(str);
}

template <>
double ston<double>(std::string str) {
  return std::stod(str);
}

template <>
float ston<float>(std::string str) {
  return std::stof(str);
}

template <>
uint8_t ston<uint8_t>(std::string str) {
  return (uint8_t)str[0];
}

template <>
uint16_t ston<uint16_t>(std::string str) {
  return std::stoi(str);
}

const static constexpr double DEFAULT_ESTIMATE = 0.0;

template <typename T>
using VarOptSketch = datasketches::var_opt_sketch<T, serde<T>>;

template <typename T>
class SketchVarOptAccumulator {
  std::unique_ptr<VarOptSketch<T>> sketch_;
  T threshold_;
  static constexpr auto headerPrefixSize = sizeof(char);
  static constexpr auto thresholdIdx = headerPrefixSize;
  static constexpr auto thresholdSize = sizeof(threshold_);
  static constexpr int headerSize = thresholdSize + headerPrefixSize;

 public:
  SketchVarOptAccumulator() {
    sketch_ = nullptr;
    threshold_ = T();
  }

  bool hasInitialized() {
    return sketch_ != nullptr;
  }

  void initialize(uint32_t k, T threshold) {
    sketch_ = std::make_unique<VarOptSketch<T>>(k);
    threshold_ = threshold;
  }

  void initialize(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    DecodedVector decodedVector;

    decodedVector.decode(*args[2], rows, true);
    T threshold = decodedVector.template valueAt<T>(0);

    decodedVector.decode(*args[1], rows, true);
    uint32_t k = decodedVector.template valueAt<uint32_t>(0);

    initialize(k, threshold);
  }

  std::string serialize() {
    std::string str;
    if (hasInitialized()) {
      auto serialized = sketch_->serialize(headerSize);
      serialized[0] = headerSize;
      *(reinterpret_cast<T*>(serialized.data() + thresholdIdx)) = threshold_;
      str = std::string(serialized.begin(), serialized.end());
    }
    return str;
  }

  static SketchVarOptAccumulator deserialize(const StringView& sv) {
    SketchVarOptAccumulator result;
    if (sv.size() > 0) {
      auto data = sv.data();
      int serializedIdx = data[0];
      result.threshold_ = *(reinterpret_cast<const T*>(data + thresholdIdx));
      auto sketch = VarOptSketch<T>::deserialize(
          data + serializedIdx, sv.size() - serializedIdx);
      result.sketch_ = std::make_unique<VarOptSketch<T>>(std::move(sketch));
    }

    return result;
  }

  void update(T t) {
    if (hasInitialized()) {
      sketch_->update(t);
    }
  }

  double getResult() {
    if (hasInitialized()) {
      datasketches::subset_summary summary =
          sketch_->estimate_subset_sum([this](T x) { return x >= threshold_; });
      return summary.estimate;
    }
    return DEFAULT_ESTIMATE;
  }

  void merge(SketchVarOptAccumulator<T>& other) {
    if (!hasInitialized() && !other.hasInitialized())
      return;

    if (!hasInitialized())
      threshold_ = other.threshold_;

    uint32_t k = hasInitialized() ? sketch_->get_k() : other.sketch_->get_k();

    datasketches::var_opt_union<T, serde<T>> u(k);
    if (hasInitialized())
      u.update(*sketch_);

    if (other.hasInitialized())
      u.update(*other.sketch_);

    VarOptSketch<T> result = u.get_result();
    sketch_ = std::make_unique<VarOptSketch<T>>(std::move(result));
  }
};

template <typename T>
class SketchVarOptAggregate
    : public SketchAggregateBase<T, SketchVarOptAccumulator<T>> {
 public:
  SketchVarOptAggregate(const TypePtr& resultType)
      : SketchAggregateBase<T, SketchVarOptAccumulator<T>>(resultType) {}

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

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BOLT_CHECK(result);
    auto rowVector = (*result)->as<RowVector>();
    auto valueVector = rowVector->childAt(0)->asFlatVector<StringView>();
    auto typeVector = rowVector->childAt(1)->asFlatVector<T>();

    rowVector->resize(numGroups);
    valueVector->resize(numGroups);
    typeVector->resize(numGroups);

    uint64_t* rawNulls = nullptr;
    if (valueVector->mayHaveNulls()) {
      BufferPtr nulls = valueVector->mutableNulls(valueVector->size());
      rawNulls = nulls->asMutable<uint64_t>();
    }

    for (auto i = 0; i < numGroups; ++i) {
      if (rawNulls) {
        bits::clearBit(rawNulls, i);
      }

      char* group = groups[i];
      auto accumulator = this->getAccumulator(group);
      auto serialized = accumulator->serialize();
      valueVector->set(i, StringView(serialized));
      typeVector->set(i, T());
    }
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decodedPairs(*args[0], rows, true);
    auto baseRowVector = dynamic_cast<const RowVector*>(decodedPairs.base());
    auto baseValueVector =
        baseRowVector->childAt(0)->as<FlatVector<StringView>>();
    auto baseTypeVector = baseRowVector->childAt(1)->as<FlatVector<T>>();

    rows.applyToSelected([&](auto row) {
      if (baseValueVector->isNullAt(row)) {
        return;
      }
      auto decodedIndex = decodedPairs.index(row);
      StringView sv = baseValueVector->valueAt(decodedIndex);
      auto temp = SketchVarOptAccumulator<T>::deserialize(sv);

      auto group = groups[row];
      this->clearNull(group);

      auto accumulator = this->getAccumulator(group);
      accumulator->merge(temp);
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decodedPairs(*args[0], rows, true);
    auto baseRowVector = dynamic_cast<const RowVector*>(decodedPairs.base());
    auto baseValueVector =
        baseRowVector->childAt(0)->as<FlatVector<StringView>>();
    auto baseTypeVector = baseRowVector->childAt(1)->as<FlatVector<T>>();

    auto accumulator = this->getAccumulator(group);
    rows.applyToSelected([&](auto row) {
      if (baseValueVector->isNullAt(row)) {
        return;
      }
      auto decodedIndex = decodedPairs.index(row);
      StringView sv = baseValueVector->valueAt(decodedIndex);
      auto temp = SketchVarOptAccumulator<T>::deserialize(sv);
      accumulator->merge(temp);
    });
  }

 private:
  DecodedVector decodedValue_;
};

template <TypeKind kind>
std::unique_ptr<exec::Aggregate> createSketchVarOptAggregate(
    const TypePtr& resultType) {
  using T = typename TypeTraits<kind>::NativeType;
  return std::make_unique<SketchVarOptAggregate<T>>(resultType);
}

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
  } else {
    BOLT_USER_CHECK_EQ(
        argTypes.size(),
        1,
        "The number of partial result for {} must be one",
        name);
    BOLT_USER_CHECK_EQ(
        argTypes[0]->childAt(0)->kind(),
        TypeKind::VARBINARY,
        "The type of partial result for {} must be VARBINARY",
        name);
  }
}

bool registerSketchVarOpt(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  for (const auto& inputType : {"tinyint", "smallint", "integer", "bigint"})
    signatures.push_back(
        exec::AggregateFunctionSignatureBuilder()
            .returnType("double")
            .intermediateType(fmt::format("row(varbinary,{})", inputType))
            .argumentType(inputType)
            .argumentType("bigint")
            .argumentType("bigint")
            .build());

  for (const auto& inputType : {"double", "real"})
    signatures.push_back(
        exec::AggregateFunctionSignatureBuilder()
            .returnType("double")
            .intermediateType(fmt::format("row(varbinary,{})", inputType))
            .argumentType(inputType)
            .argumentType("bigint")
            .argumentType("double")
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

        checkInputs(isRawInput, name, argTypes);

        if (!isRawInput && exec::isPartialOutput(step)) {
          return std::make_unique<SketchVarOptAggregate<uint8_t>>(VARBINARY());
        }

        TypePtr type = isRawInput ? argTypes[0] : argTypes[0]->childAt(1);

        return BOLT_DYNAMIC_SCALAR_SKETCH_TYPE_DISPATCH(
            createSketchVarOptAggregate, type->kind(), resultType);
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerSketchVarOpt(kVarOpt);

} // namespace

void registerSketchVarOptAggregate() {
  registerSketchVarOpt(kVarOpt);
}
} // namespace bytedance::bolt::aggregate
