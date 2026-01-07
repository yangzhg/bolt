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
class SketchHLLUnionAccumulator {
  const static datasketches::target_hll_type DEFAULT_HLL_TYPE =
      datasketches::HLL_4;
  const static int8_t DEFAULT_LOG_K = 12;

  std::unique_ptr<HLLUnion> union_;

 public:
  void initialize(uint8_t logK) {
    union_ = std::make_unique<HLLUnion>(logK);
  }

  void initialize() {
    initialize(DEFAULT_LOG_K);
  }

  void initialize(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {}

  SketchHLLUnionAccumulator() {
    union_ = nullptr;
  }

  bool hasInitialized() const {
    return union_ != nullptr;
  }

  std::string getResult() const {
    return serialize();
  }

  std::string serialize() const {
    if (!hasInitialized())
      return "";
    auto serialized = union_->get_result().serialize_compact();
    return std::string(serialized.begin(), serialized.end());
  }

  static SketchHLLUnionAccumulator deserialize(const StringView& sv) {
    SketchHLLUnionAccumulator result;
    if (sv.empty())
      return result;
    auto sketch = HLLSketch::deserialize(sv.data(), sv.size());
    result.union_ = std::make_unique<HLLUnion>(sketch.get_lg_config_k());
    result.union_->update(sketch);
    return result;
  }

  void update(const StringView& binarySketch) {
    if (binarySketch.empty()) {
      VLOG(0) << "Serialized varbinary is empty.";
      return;
    }
    auto sketch =
        HLLSketch::deserialize(binarySketch.data(), binarySketch.size());
    if (!hasInitialized()) {
      union_ = std::make_unique<HLLUnion>(sketch.get_lg_config_k());
    }
    union_->update(sketch);
  }

  void merge(const SketchHLLUnionAccumulator& other) {
    if (!other.hasInitialized())
      return;

    if (!hasInitialized()) {
      union_ = std::make_unique<HLLUnion>(other.union_->get_lg_config_k());
    }
    union_->update(other.union_->get_result());
  }
};

class SketchesHLLUnionAggregate
    : public SketchAggregateBase<StringView, SketchHLLUnionAccumulator> {
 public:
  SketchesHLLUnionAggregate(const TypePtr& resultType)
      : SketchAggregateBase<StringView, SketchHLLUnionAccumulator>(resultType) {
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BOLT_CHECK(result);
    FlatVector<StringView>* flatResult =
        (*result)->as<FlatVector<StringView>>();
    flatResult->resize(numGroups);

    uint64_t* rawNulls = nullptr;
    if (flatResult->mayHaveNulls()) {
      BufferPtr nulls = flatResult->mutableNulls(flatResult->size());
      rawNulls = nulls->asMutable<uint64_t>();
    }

    for (auto i = 0; i < numGroups; ++i) {
      if (rawNulls) {
        bits::clearBit(rawNulls, i);
      }

      char* group = groups[i];
      auto accumulator = getAccumulator(group);
      auto serialized = accumulator->serialize();
      flatResult->set(i, StringView(serialized));
    }
  }
};

template <TypeKind kind>
std::unique_ptr<exec::Aggregate> createSketchHLLUnionAggregate(
    const TypePtr& resultType) {
  using T = typename TypeTraits<kind>::NativeType;
  return std::make_unique<SketchesHLLUnionAggregate>(resultType);
}

bool registerHLL(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("varbinary")
                           .intermediateType("varbinary")
                           .argumentType("varbinary")
                           .argumentType("BIGINT")
                           .argumentType("BIGINT")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("varbinary")
                           .intermediateType("varbinary")
                           .argumentType("varbinary")
                           .argumentType("BIGINT")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType("varbinary")
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
          BOLT_USER_CHECK_LT(
              argTypes.size(),
              4,
              "Wrong number of arguments passed to {}",
              name);
          BOLT_USER_CHECK_EQ(
              argTypes[0]->kind(),
              TypeKind::VARBINARY,
              "The type of first argument for {} must be VARBINARY",
              name);
          if (argTypes.size() == 2) {
            BOLT_USER_CHECK_EQ(
                argTypes[1]->kind(),
                TypeKind::BIGINT,
                "The type of second argument for {} must be tiny int",
                name);
          }
          if (argTypes.size() == 3) {
            BOLT_USER_CHECK_EQ(
                argTypes[2]->kind(),
                TypeKind::BIGINT,
                "The type of third argument for {} must be tiny int",
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

        if (!isRawInput && exec::isPartialOutput(step)) {
          return std::make_unique<SketchesHLLUnionAggregate>(VARBINARY());
        }

        TypePtr type = isRawInput ? argTypes[0] : resultType;

        return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
            createSketchHLLUnionAggregate, type->kind(), resultType);
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerHLL(kHLLUnionSketches);

} // namespace

void registerHLLUnionSketchesAggregate() {
  registerHLL(kHLLUnionSketches);
}
} // namespace bytedance::bolt::aggregate
