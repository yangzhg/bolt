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
using HLLUnion = datasketches::hll_union;

template <bool needRound>
class SketchHLLSketchAccumulator {
  const static constexpr double DEFAULT_ESTIMATE = 0.0;

  // Store the union object directly to avoid re-creating it on every update.
  std::unique_ptr<HLLUnion> union_;

  // Store sketch parameters, as they are needed to get the final result.
  uint8_t logK_;
  datasketches::target_hll_type type_;

  std::optional<bool> isStable_;

 public:
  // This is a placeholder for API consistency; it's not directly called
  // for this specific aggregation type which initializes lazily.
  void initialize(uint8_t logK, datasketches::target_hll_type type) {
    logK_ = logK;
    type_ = type;
    union_ = std::make_unique<HLLUnion>(logK);
  }

  void initialize(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    if (args.size() > 1) {
      DecodedVector decodedVector(*args[1], rows, true);
      if (!decodedVector.isConstantMapping()) {
        BOLT_USER_FAIL(
            "isStable argument must be a constant for all rows in a group");
      }
      if (!decodedVector.isNullAt(rows.begin())) {
        isStable_ = decodedVector.valueAt<bool>(rows.begin());
      }
    }
  }

  SketchHLLSketchAccumulator()
      : union_{nullptr},
        logK_{0},
        type_{datasketches::HLL_4}, // A default, will be overwritten
        isStable_{std::nullopt} {}

  bool hasInitialized() const {
    return union_ != nullptr;
  }

  double getResult() const {
    if (hasInitialized()) {
      double out;
      auto resultSketch = union_->get_result(type_);
      if (isStable_.value_or(false)) {
        out = resultSketch.get_composite_estimate();
      } else {
        out = resultSketch.get_estimate();
      }
      if constexpr (needRound) {
        return std::round(out);
      }
      return out;
    }
    return DEFAULT_ESTIMATE;
  }

  std::string serialize() {
    if (!hasInitialized()) {
      return "";
    }
    auto resultSketch = union_->get_result(type_);
    auto serialized = resultSketch.serialize_compact();
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

  static SketchHLLSketchAccumulator deserialize(const StringView& sv) {
    SketchHLLSketchAccumulator result;
    if (sv.empty()) {
      return result;
    }

    const char* data = sv.data();
    size_t size = sv.size();

    if (size > 2 && data[0] == 'v') {
      result.isStable_ = (data[1] == 1);
      data += 2;
      size -= 2;
    }

    auto sketch = HLLSketch::deserialize(data, size);
    result.logK_ = sketch.get_lg_config_k();
    result.type_ = sketch.get_target_type();
    result.union_ = std::make_unique<HLLUnion>(result.logK_);
    result.union_->update(sketch);
    return result;
  }

  void update(const StringView& t) {
    if (t.empty()) {
      return;
    }
    // Deserialize the input sketch once.
    auto sketch = HLLSketch::deserialize(t.data(), t.size());
    if (!hasInitialized()) {
      // Lazily initialize the union on the first update.
      logK_ = sketch.get_lg_config_k();
      type_ = sketch.get_target_type();
      union_ = std::make_unique<HLLUnion>(logK_);
    }
    // Efficiently update the existing union.
    union_->update(sketch);
  }

  void merge(const SketchHLLSketchAccumulator& other) {
    if (!other.hasInitialized()) {
      return;
    }

    if (!hasInitialized()) {
      // If this accumulator is empty, "clone" the other one.
      logK_ = other.logK_;
      type_ = other.type_;
      isStable_ = other.isStable_;
      union_ = std::make_unique<HLLUnion>(logK_);
      auto otherSketch = other.union_->get_result(other.type_);
      union_->update(otherSketch);
      return;
    }

    if (!isStable_.has_value() && other.isStable_.has_value()) {
      isStable_ = other.isStable_;
    }

    // Corrected merge logic: get sketch from other and update this union.
    auto otherSketch = other.union_->get_result(other.type_);
    union_->update(otherSketch);
  }
};

template <bool needRound>
class SketchesHLLAggregate : public SketchAggregateBase<
                                 StringView,
                                 SketchHLLSketchAccumulator<needRound>> {
 public:
  SketchesHLLAggregate(const TypePtr& resultType)
      : SketchAggregateBase<StringView, SketchHLLSketchAccumulator<needRound>>(
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

std::unique_ptr<exec::Aggregate> createSketchHLLAggregate(
    const TypePtr& resultType,
    const bool needRound) {
  if (needRound) {
    return std::make_unique<SketchesHLLAggregate<true>>(resultType);
  } else {
    return std::make_unique<SketchesHLLAggregate<false>>(resultType);
  }
}

bool registerHLLSketch(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType("varbinary")
          .argumentType("varbinary")
          .build(),
      exec::AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType("varbinary")
          .argumentType("varbinary")
          .argumentType("boolean")
          .build()};

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
          BOLT_USER_CHECK_LE(
              argTypes.size(),
              2,
              "Wrong number of arguments passed to {}",
              name);
          BOLT_USER_CHECK_EQ(
              argTypes[0]->kind(),
              TypeKind::VARBINARY,
              "The type of first argument for {} must be varbinary",
              name);
          if (argTypes.size() > 1) {
            BOLT_USER_CHECK_EQ(
                argTypes[1]->kind(),
                TypeKind::BOOLEAN,
                "The type of second argument for {} must be boolean",
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
            return std::make_unique<SketchesHLLAggregate<true>>(VARBINARY());
          } else {
            return std::make_unique<SketchesHLLAggregate<false>>(VARBINARY());
          }
        }

        return createSketchHLLAggregate(resultType, needRound);
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerHLLSketch(kHLLSketch);

} // namespace

void registerHLLSketchAggregate() {
  registerHLLSketch(kHLLSketch);
}
} // namespace bytedance::bolt::aggregate
