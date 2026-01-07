/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/expression/DecodedArgs.h"
#include "bolt/expression/EvalCtx.h"
#include "bolt/expression/VectorFunction.h"
namespace bytedance::bolt::functions {
namespace {

template <bool IsNotNULL>
class IsNullFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto* arg = args[0].get();
    auto* pool = context.pool();
    if (arg->isConstantEncoding()) {
      bool isNull = arg->isNullAt(rows.begin());
      auto localResult = BaseVector::createConstant(
          BOOLEAN(), IsNotNULL ? !isNull : isNull, rows.end(), pool);
      context.moveOrCopyResult(localResult, rows, result);
      return;
    }

    if (!arg->mayHaveNulls()) {
      // No nulls.
      auto localResult = BaseVector::createConstant(
          BOOLEAN(), IsNotNULL ? true : false, rows.end(), pool);
      context.moveOrCopyResult(localResult, rows, result);
      return;
    }

    BufferPtr isNull;
    if (arg->isFlatEncoding()) {
      if constexpr (IsNotNULL) {
        isNull = arg->nulls();
      } else {
        isNull = AlignedBuffer::allocate<bool>(rows.end(), pool);
        memcpy(
            isNull->asMutable<int64_t>(),
            arg->rawNulls(),
            bits::nbytes(rows.end()));
        bits::negate(isNull->asMutable<char>(), rows.end());
      }
    } else {
      exec::DecodedArgs decodedArgs(rows, args, context);

      isNull = AlignedBuffer::allocate<bool>(rows.end(), pool);
      memcpy(
          isNull->asMutable<int64_t>(),
          decodedArgs.at(0)->nulls(&rows),
          bits::nbytes(rows.end()));

      if (!IsNotNULL) {
        bits::negate(isNull->asMutable<char>(), rows.end());
      }
    }

    auto localResult = std::make_shared<FlatVector<bool>>(
        pool, BOOLEAN(), nullptr, rows.end(), isNull, std::vector<BufferPtr>{});
    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T -> boolean
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("boolean")
                .argumentType("T")
                .build()};
  }
};
} // namespace

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_is_null,
    IsNullFunction<false>::signatures(),
    std::make_unique<IsNullFunction</*IsNotNUll=*/false>>());

void registerIsNullFunction(const std::string& name) {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_is_null, name);
}

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_is_not_null,
    IsNullFunction<true>::signatures(),
    std::make_unique<IsNullFunction</*IsNotNUll=*/true>>());

void registerIsNotNullFunction(const std::string& name) {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_is_not_null, name);
}

} // namespace bytedance::bolt::functions
