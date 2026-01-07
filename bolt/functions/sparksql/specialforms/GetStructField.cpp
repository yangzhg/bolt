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

#include "bolt/functions/sparksql/specialforms/GetStructField.h"
#include <type/Type.h>
#include "bolt/expression/PeeledEncoding.h"
#include "expression/Expr.h"
#include "vector/ComplexVector.h"
#include "vector/ConstantVector.h"
using namespace bytedance::bolt::exec;
namespace bytedance::bolt::functions::sparksql {

void GetStructFieldExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  VectorPtr dataVec;
  VectorPtr ordinalVec;
  inputs_[0]->eval(rows, context, dataVec);
  inputs_[1]->eval(rows, context, ordinalVec);
  auto ordinal = ordinalVec->as<ConstantVector<int32_t>>();
  auto resultType = std::const_pointer_cast<const Type>(type_);
  BOLT_CHECK(ordinal != nullptr);
  LocalSelectivityVector remainingRows(context, rows);
  context.deselectErrors(*remainingRows);
  LocalDecodedVector decoded(context, *dataVec, *remainingRows);
  auto* rawNulls = decoded->nulls(&rows);

  if (rawNulls) {
    remainingRows->deselectNulls(
        rawNulls, remainingRows->begin(), remainingRows->end());
  }
  VectorPtr localResult;
  if (!remainingRows->hasSelections()) {
    localResult =
        BaseVector::createNullConstant(resultType, rows.end(), context.pool());
  } else if (decoded->isIdentityMapping()) {
    auto data = decoded->base()->as<RowVector>();
    BOLT_CHECK(data != nullptr);
    BOLT_CHECK(ordinal->valueAt(0) < data->childrenSize());
    localResult = data->childAt(ordinal->valueAt(0));
  } else {
    withContextSaver([&](ContextSaver& saver) {
      LocalSelectivityVector newRowsHolder(*context.execCtx());
      LocalDecodedVector localDecoded(context);
      std::vector<VectorPtr> peeledVectors;
      auto peeledEncoding = PeeledEncoding::peel(
          {dataVec}, *remainingRows, localDecoded, true, peeledVectors);
      BOLT_CHECK_EQ(peeledVectors.size(), 1);
      if (peeledVectors[0]->isLazy()) {
        peeledVectors[0] =
            peeledVectors[0]->as<LazyVector>()->loadedVectorShared();
      }
      auto newRows =
          peeledEncoding->translateToInnerRows(*remainingRows, newRowsHolder);
      context.saveAndReset(saver, *remainingRows);
      context.setPeeledEncoding(peeledEncoding);
      auto data = peeledVectors[0]->as<RowVector>();
      BOLT_CHECK(data != nullptr);
      BOLT_CHECK(ordinal->valueAt(0) < data->childrenSize());
      localResult = data->childAt(ordinal->valueAt(0));
      localResult = context.getPeeledEncoding()->wrap(
          resultType, context.pool(), localResult, *remainingRows);
    });
  }
  context.moveOrCopyResult(localResult, *remainingRows, result);
  context.releaseVector(localResult);

  BOLT_CHECK_NOT_NULL(result);
  if (rawNulls || context.errors()) {
    EvalCtx::addNulls(
        rows, remainingRows->asRange().bits(), context, resultType, result);
  }

  context.releaseVector(dataVec);
  context.releaseVector(ordinalVec);
}

TypePtr GetStructFieldToSpecialForm::resolveType(
    const std::vector<TypePtr>& argTypes) {
  BOLT_FAIL("get_struct_field expressions do not support type resolution.");
}

ExprPtr GetStructFieldToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const core::QueryConfig& config) {
  if (compiledChildren.size() != 2) {
    BOLT_FAIL(
        "get_struct_field statements expect two argument, received {}.",
        compiledChildren.size());
  }
  return std::make_shared<GetStructFieldExpr>(
      type, std::move(compiledChildren), trackCpuUsage);
}

} // namespace bytedance::bolt::functions::sparksql