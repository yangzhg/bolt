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

#include <DataSketches/kll_sketch.hpp>

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
using KLLSketch = datasketches::kll_sketch<T, std::less<T>, serde<T>>;

template <typename T>
class SketchKLLSketchAccumulator {
  std::unique_ptr<KLLSketch<T>> sketch_;
  double rank_;
  static constexpr auto headerPrefixSize = sizeof(char);
  static constexpr auto rankIdx = headerPrefixSize;
  static constexpr auto rankSize = sizeof(rank_);
  static constexpr auto typeIdx = rankIdx + rankSize;
  static constexpr auto typeSize = 2;
  static constexpr int headerSize = rankSize + headerPrefixSize + typeSize;

 public:
  SketchKLLSketchAccumulator() {
    sketch_ = nullptr;
    rank_ = 0;
  }

  void initialize(uint16_t k, double rank) {
    sketch_ = std::make_unique<KLLSketch<T>>(k);
    rank_ = rank;
  }

  void initialize(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    DecodedVector decodedVector;

    decodedVector.decode(*args[2], rows, true);
    double rank = decodedVector.template valueAt<double>(0);

    decodedVector.decode(*args[1], rows, true);
    uint16_t k = decodedVector.template valueAt<uint16_t>(0);

    initialize(k, rank);
  }

  bool hasInitialized() const {
    return sketch_ != nullptr;
  }

  T getResult() const {
    if (hasInitialized())
      return sketch_->get_quantile(rank_);

    return T();
  }

  std::string serialize() {
    if (!hasInitialized())
      return "";

    if (typeNameMap.find(typeid(T).name()) == typeNameMap.end())
      return "";
    constexpr int typeNameIdx = sizeof(double) + rankIdx;
    auto typeName = typeNameMap[typeid(T).name()];
    BOLT_CHECK_LE(typeName.length(), 2);
    auto serialized = sketch_->serialize(headerSize);
    serialized[0] = headerSize;
    *(reinterpret_cast<double*>(serialized.data() + rankIdx)) = rank_;
    serialized[typeNameIdx] =
        typeName.length() == 1 ? ' ' : typeName[typeName.length() - 2];
    serialized[typeNameIdx + 1] = typeName[typeName.length() - 1];
    auto str = std::string(serialized.begin(), serialized.end());
    return str;
  }

  static SketchKLLSketchAccumulator deserialize(const StringView& sv) {
    SketchKLLSketchAccumulator result;
    if (sv.empty())
      return SketchKLLSketchAccumulator();

    try {
      auto data = sv.data();
      int serializedIdx = data[0];
      result.rank_ = *(reinterpret_cast<const double*>(data + rankIdx));
      auto typeLen = data[typeIdx + 1] == ' ' ? 1 : 2;
      std::string type = std::string(sv.begin() + typeIdx, typeLen);
      auto sketch = KLLSketch<T>::deserialize(
          data + serializedIdx, sv.size() - serializedIdx);
      result.sketch_ = std::make_unique<KLLSketch<T>>(std::move(sketch));
    } catch (...) {
      BOLT_CHECK(false, "Failure to deserialize SketchKLLSketchAccumulator.");
    }

    return result;
  }

  void update(const StringView& sv) {
    auto data = sv.data();
    auto size = sv.size();
    int serializedIdx = size > 0 ? data[0] : 0;
    if (size > serializedIdx) {
      auto sketch =
          KLLSketch<T>::deserialize(data + serializedIdx, size - serializedIdx);

      if (!hasInitialized()) {
        sketch_ = std::make_unique<KLLSketch<T>>(std::move(sketch));
      } else {
        sketch_->template merge(sketch);
      }
    }
  }

  void merge(const SketchKLLSketchAccumulator& other) {
    if (!other.hasInitialized())
      return;

    if (!hasInitialized()) {
      initialize(other.sketch_->get_k(), other.rank_);
    }

    sketch_->template merge(*other.sketch_);
  }
};

template <typename T>
class SketchesHLLAggregate
    : public SketchAggregateBase<StringView, SketchKLLSketchAccumulator<T>> {
 public:
  SketchesHLLAggregate(const TypePtr& resultType)
      : SketchAggregateBase<StringView, SketchKLLSketchAccumulator<T>>(
            resultType) {}

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
  }
}

template <typename T>
void registerKLLSketch(const std::string& name, const std::string& returnType) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType("varbinary")
                           .argumentType("varbinary")
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

        BOLT_USER_CHECK_EQ(
            argTypes[0]->kind(),
            TypeKind::VARBINARY,
            "The type of partial result for {} must be VARBINARY",
            name);

        checkInputs(isRawInput, name, argTypes);

        if (!isRawInput && exec::isPartialOutput(step)) {
          return std::make_unique<SketchesHLLAggregate<T>>(VARBINARY());
        }

        return std::make_unique<SketchesHLLAggregate<T>>(resultType);
      });
}

bool registerKLLSketches() {
  registerKLLSketch<bool>(kKLLSketchBool, "boolean");
  registerKLLSketch<int8_t>(kKLLSketchChar, "tinyint");
  registerKLLSketch<int16_t>(kKLLSketchShort, "smallint");
  registerKLLSketch<int32_t>(kKLLSketchInt, "integer");
  registerKLLSketch<int64_t>(kKLLSketchLong, "bigint");
  registerKLLSketch<float>(kKLLSketchFloat, "real");
  registerKLLSketch<double>(kKLLSketchDouble, "double");
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) = registerKLLSketches();

} // namespace

void registerKLLSketchesAggregate() {
  registerKLLSketches();
}
} // namespace bytedance::bolt::aggregate
