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

#include "bolt/expression/Expr.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/lib/LambdaFunctionUtil.h"
#include "bolt/functions/lib/RowsTranslationUtil.h"
#include "bolt/vector/FunctionVector.h"
namespace bytedance::bolt::functions {
namespace {

// See documentation at https://prestodb.io/docs/current/functions/array.html
class TransformFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    // transform is null preserving for the array. But since an
    // expr tree with a lambda depends on all named fields, including
    // captures, a null in a capture does not automatically make a
    // null result.
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK_EQ(args.size(), 2);

    // Flatten input array.
    exec::LocalDecodedVector arrayDecoder(context, *args[0], rows);
    auto& decodedArray = *arrayDecoder.get();

    auto flatArray = flattenArray(rows, args[0], decodedArray);

    std::vector<VectorPtr> lambdaArgs = {flatArray->elements()};
    auto newNumElements = flatArray->elements()->size();

    SelectivityVector validRowsInReusedResult =
        toElementRows<ArrayVector>(newNumElements, rows, flatArray.get());

    // transformed elements
    VectorPtr newElements;

    auto elementToTopLevelRows = getElementToTopLevelRows(
        newNumElements, rows, flatArray.get(), context.pool());

    // loop over lambda functions and apply these to elements of the base array;
    // in most cases there will be only one function and the loop will run once
    auto it = args[1]->asUnchecked<FunctionVector>()->iterator(&rows);
    while (auto entry = it.next()) {
      auto elementRows = toElementRows<ArrayVector>(
          newNumElements, *entry.rows, flatArray.get());
      auto wrapCapture = toWrapCapture<ArrayVector>(
          newNumElements, entry.callable, *entry.rows, flatArray);

      entry.callable->apply(
          elementRows,
          &validRowsInReusedResult,
          wrapCapture,
          &context,
          lambdaArgs,
          elementToTopLevelRows,
          &newElements);
    }

    // Set nulls for rows not present in 'rows'.
    BufferPtr newNulls = addNullsForUnselectedRows(flatArray, rows);

    VectorPtr localResult = std::make_shared<ArrayVector>(
        flatArray->pool(),
        outputType,
        std::move(newNulls),
        flatArray->size(),
        flatArray->offsets(),
        flatArray->sizes(),
        newElements);
    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // array(T), function(T, U) -> array(U)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .typeVariable("U")
                .returnType("array(U)")
                .argumentType("array(T)")
                .argumentType("function(T, U)")
                .build()};
  }
};
} // namespace

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_transform,
    TransformFunction::signatures(),
    std::make_unique<TransformFunction>());

} // namespace bytedance::bolt::functions
