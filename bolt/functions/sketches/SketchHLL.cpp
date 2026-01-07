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

#include <cmath>
#include <memory>

#include <DataSketches/hll.hpp>

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

using HLLSketch = datasketches::hll_sketch;
template <bool needRound>
class SketchHLLAccumulator {
  const static constexpr double DEFAULT_ESTIMATE = 0.0;
  std::unique_ptr<HLLSketch> sketch_;

  std::optional<bool> isStable_;

 public:
  void initialize(uint8_t logK, datasketches::target_hll_type type) {
    sketch_ = std::make_unique<HLLSketch>(logK, type);
  }

  void initialize(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    DecodedVector decodedVector;
    decodedVector.decode(*args[1], rows, true);
    uint8_t logK = decodedVector.template valueAt<uint8_t>(0);

    decodedVector.decode(*args[2], rows, true);
    auto type =
        (datasketches::target_hll_type)decodedVector.template valueAt<int8_t>(
            0);
    initialize(logK, type);

    // args[3] corresponds to the 'isStable' boolean flag.
    if (args.size() > 3) {
      DecodedVector decodedVector(*args[3], rows, true);
      if (!decodedVector.isConstantMapping()) {
        BOLT_USER_FAIL(
            "isStable argument must be a constant for all rows in a group");
      }
      if (!decodedVector.isNullAt(rows.begin())) {
        isStable_ = decodedVector.valueAt<bool>(rows.begin());
      }
    }
  }

  SketchHLLAccumulator() : sketch_{nullptr}, isStable_{std::nullopt} {}

  bool hasInitialized() const {
    return sketch_ != nullptr;
  }

  double getResult() const {
    if (hasInitialized()) {
      double out;
      if (isStable_.value_or(false)) {
        out = sketch_->get_composite_estimate();
      } else {
        out = sketch_->get_estimate();
      }
      if constexpr (needRound) {
        return std::round(out);
      }
      return out;
    }
    return DEFAULT_ESTIMATE;
  }

  std::string serialize() {
    if (!hasInitialized())
      return "";

    auto serialized = sketch_->serialize_compact();
    if (isStable_.has_value()) {
      std::string result;
      result.reserve(serialized.size() + 2);
      result.push_back('v'); // version marker
      result.push_back(isStable_.value() ? 1 : 0);
      result.append(
          reinterpret_cast<const char*>(serialized.data()), serialized.size());
      return result;
    }
    return std::string(
        reinterpret_cast<const char*>(serialized.data()), serialized.size());
  }

  static SketchHLLAccumulator deserialize(const StringView& sv) {
    SketchHLLAccumulator result;
    if (sv.empty())
      return result;

    const char* data = sv.data();
    size_t size = sv.size();

    if (size > 2 && data[0] == 'v') {
      result.isStable_ = (data[1] == 1);
      data += 2;
      size -= 2;
    }

    auto sketch = HLLSketch::deserialize(data, size);
    result.sketch_ = std::make_unique<HLLSketch>(std::move(sketch));
    return result;
  }

  template <typename T>
  void update(T t) {
    if (hasInitialized()) {
      sketch_->update(t);
    }
  }

  void merge(const SketchHLLAccumulator& other) {
    if (!other.hasInitialized()) {
      return;
    }

    if (!hasInitialized()) {
      sketch_ = std::make_unique<HLLSketch>(*other.sketch_);
      isStable_ = other.isStable_;
      return;
    }

    if (!isStable_.has_value() && other.isStable_.has_value()) {
      isStable_ = other.isStable_;
    }

    datasketches::hll_union u(sketch_->get_lg_config_k());
    u.update(*sketch_);
    u.update(*other.sketch_);
    auto result = u.get_result();
    sketch_ = std::make_unique<HLLSketch>(std::move(result));
  }
};

template <typename T, bool needRound>
class SketchesHLLAggregate
    : public SketchAggregateBase<T, SketchHLLAccumulator<needRound>> {
 public:
  SketchesHLLAggregate(const TypePtr& resultType)
      : SketchAggregateBase<T, SketchHLLAccumulator<needRound>>(resultType) {}

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
std::unique_ptr<exec::Aggregate> createSketchHLLAggregate(
    const TypePtr& resultType,
    bool needRound) {
  using T = typename TypeTraits<kind>::NativeType;
  if (needRound) {
    return std::make_unique<SketchesHLLAggregate<T, true>>(resultType);
  }
  return std::make_unique<SketchesHLLAggregate<T, false>>(resultType);
}

bool registerHLL(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  for (const auto& inputType : primitiveTypes) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("double")
                             .intermediateType("varbinary")
                             .argumentType(inputType)
                             .argumentType("BIGINT")
                             .argumentType("BIGINT")
                             .build());
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType("double")
                             .intermediateType("varbinary")
                             .argumentType(inputType)
                             .argumentType("BIGINT")
                             .argumentType("BIGINT")
                             .argumentType("BOOLEAN")
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

        if (isRawInput) {
          BOLT_USER_CHECK(
              argTypes.size() == 3 || argTypes.size() == 4,
              "Wrong number of arguments passed to {}",
              name);
          BOLT_USER_CHECK_EQ(
              argTypes[1]->kind(),
              TypeKind::BIGINT,
              "The type of second argument for {} must be BIGINT",
              name);
          BOLT_USER_CHECK_EQ(
              argTypes[2]->kind(),
              TypeKind::BIGINT,
              "The type of third argument for {} must be BIGINT",
              name);
          if (argTypes.size() > 3) {
            BOLT_USER_CHECK_EQ(
                argTypes[3]->kind(),
                TypeKind::BOOLEAN,
                "The type of fourth argument for {} must be boolean",
                name);
          }

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

        const bool needRound = config.hllSketchRounded();

        if (!isRawInput && exec::isPartialOutput(step)) {
          if (needRound) {
            return std::make_unique<SketchesHLLAggregate<uint8_t, true>>(
                VARBINARY());
          }
          return std::make_unique<SketchesHLLAggregate<uint8_t, false>>(
              VARBINARY());
        }

        TypePtr type = isRawInput ? argTypes[0] : resultType;

        return BOLT_DYNAMIC_SCALAR_SKETCH_TYPE_DISPATCH(
            createSketchHLLAggregate, type->kind(), resultType, needRound);
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) = registerHLL(kHLL);

} // namespace

void registerHLLAggregate() {
  registerHLL(kHLL);
}
} // namespace bytedance::bolt::aggregate
