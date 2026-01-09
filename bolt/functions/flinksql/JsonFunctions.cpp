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

#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/prestosql/types/JsonType.h"
#include "bolt/vector/ConstantVector.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::functions::flinksql {
namespace {
template <typename NativeType>
VectorPtr checkAndFlatten(const SelectivityVector& rows, VectorPtr& input) {
  BOLT_CHECK_NOT_NULL(input);
  if (input->as<SimpleVector<NativeType>>()) {
    return input;
  }
  DecodedVector decoded(*input, rows);
  auto flatVector = BaseVector::create<FlatVector<NativeType>>(
      input->type(), decoded.size(), input->pool());
  if (std::is_same_v<NativeType, StringView>) {
    flatVector->acquireSharedStringBuffers(input.get());
  }
  if (decoded.mayHaveNulls()) {
    rows.applyToSelected([&](vector_size_t row) {
      if (decoded.isNullAt(row)) {
        flatVector->setNull(row, true);
      } else if constexpr (std::is_same_v<NativeType, StringView>) {
        flatVector->setNoCopy(row, decoded.valueAt<NativeType>(row));
      } else {
        flatVector->set(row, decoded.valueAt<NativeType>(row));
      }
    });
  } else {
    rows.applyToSelected([&](vector_size_t row) {
      if constexpr (std::is_same_v<NativeType, StringView>) {
        flatVector->setNoCopy(row, decoded.valueAt<NativeType>(row));
      } else {
        flatVector->set(row, decoded.valueAt<NativeType>(row));
      }
    });
  }
  return flatVector;
}

class JsonStrToMapFunction : public bytedance::bolt::exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto castFactory =
        dynamic_cast<const JsonCastOperator*>(JsonCastOperator::get().get());
    VectorPtr logFailuresOnly;
    if (args.size() == 2) {
      logFailuresOnly = checkAndFlatten<bool>(rows, args[1]);
    }
    castFactory->castFrom(
        *checkAndFlatten<StringView>(rows, args[0]),
        context,
        rows,
        outputType,
        result,
        false,
        std::move(logFailuresOnly));
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        exec::FunctionSignatureBuilder()
            .returnType("map(varchar,varchar)")
            .argumentType("varchar")
            .build(),
        exec::FunctionSignatureBuilder()
            .returnType("map(varchar,varchar)")
            .argumentType("varchar")
            .argumentType("boolean")
            .build()};
  }
};

class JsonStrToArrayFunction : public bytedance::bolt::exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto castFactory =
        dynamic_cast<const JsonCastOperator*>(JsonCastOperator::get().get());
    VectorPtr logFailuresOnly;
    if (args.size() == 2) {
      logFailuresOnly = checkAndFlatten<bool>(rows, args[1]);
    }
    castFactory->castFrom(
        *checkAndFlatten<StringView>(rows, args[0]),
        context,
        rows,
        outputType,
        result,
        false,
        std::move(logFailuresOnly));
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        exec::FunctionSignatureBuilder()
            .returnType("array(varchar)")
            .argumentType("varchar")
            .build(),
        exec::FunctionSignatureBuilder()
            .returnType("array(varchar)")
            .argumentType("varchar")
            .argumentType("boolean")
            .build()};
  }
};
} // namespace

BOLT_DECLARE_VECTOR_FUNCTION(
    json_str_to_map,
    JsonStrToMapFunction::signatures(),
    std::make_unique<JsonStrToMapFunction>());
BOLT_DECLARE_VECTOR_FUNCTION(
    json_str_to_array,
    JsonStrToArrayFunction::signatures(),
    std::make_unique<JsonStrToArrayFunction>());
} // namespace bytedance::bolt::functions::flinksql
