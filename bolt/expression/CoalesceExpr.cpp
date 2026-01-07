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

#include <algorithm>

#include "bolt/common/base/Exceptions.h"
#include "bolt/expression/CoalesceExpr.h"
#include "bolt/expression/FieldReference.h"
#include "bolt/vector/BaseVector.h"
namespace bytedance::bolt::exec {

template <bool areSimpleInputs>
CoalesceExpr<areSimpleInputs>::CoalesceExpr(
    TypePtr type,
    std::vector<ExprPtr>&& inputs,
    bool inputsSupportFlatNoNullsFastPath)
    : SpecialForm(
          std::move(type),
          std::move(inputs),
          kCoalesce,
          inputsSupportFlatNoNullsFastPath,
          false /* trackCpuUsage */) {
  std::vector<TypePtr> inputTypes;
  inputTypes.reserve(inputs_.size());
  std::transform(
      inputs_.begin(),
      inputs_.end(),
      std::back_inserter(inputTypes),
      [](const ExprPtr& expr) { return expr->type(); });

  // Apply type checks.
  auto expectedType = resolveType(inputTypes);
  BOLT_CHECK(
      *expectedType == *this->type(),
      "Coalesce expression type different than its inputs. Expected {} but got Actual {}.",
      expectedType->toString(),
      this->type()->toString());
}

template <bool areSimpleInputs>
void CoalesceExpr<areSimpleInputs>::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  // Null positions to populate.
  exec::LocalSelectivityVector activeRowsHolder(context, rows.end());
  auto activeRows = activeRowsHolder.get();
  assert(activeRows); // for lint
  *activeRows = rows;

  if constexpr (areSimpleInputs) {
    // simple inputs should copy vectors to result, which don't overwrite result
    // when evaluating, so copy in this loop and ensureWritable only once.
    for (int i = 0; i < inputs_.size(); i++) {
      VectorPtr tempResult;
      inputs_[i]->eval(*activeRows, context, tempResult);
      if (i == 0) {
        if (result == nullptr && !tempResult->mayHaveNulls()) {
          result = std::move(tempResult);
          return;
        } else {
          context.ensureWritable(rows, type(), result);
          result->copy(
              tempResult.get(),
              *activeRows,
              nullptr,
              context.isFinalSelection());
        }
      } else {
        result->copy(tempResult.get(), *activeRows, nullptr, false);
        if (!result->mayHaveNulls()) {
          // No nulls left.
          return;
        }
      }

      if (context.errors()) {
        context.deselectErrors(*activeRows);
      }

      const uint64_t* rawNulls = result->rawNulls();
      if (!rawNulls) {
        // No nulls left.
        return;
      }

      activeRows->deselectNonNulls(rawNulls, 0, activeRows->end());
      if (!activeRows->hasSelections()) {
        // No nulls left.
        return;
      }
    }
    return;
  }
  // Fix finalSelection at "rows" unless already fixed.
  ScopedFinalSelectionSetter scopedFinalSelectionSetter(context, &rows);

  exec::LocalDecodedVector decodedVector(context);
  for (int i = 0; i < inputs_.size(); i++) {
    inputs_[i]->eval(*activeRows, context, result);

    if (!result->mayHaveNulls()) {
      // No nulls left.
      return;
    }

    if (context.errors()) {
      context.deselectErrors(*activeRows);
    }

    decodedVector.get()->decode(*result, *activeRows);
    const uint64_t* rawNulls = decodedVector->nulls(activeRows);
    if (!rawNulls) {
      // No nulls left.
      return;
    }

    activeRows->deselectNonNulls(rawNulls, 0, activeRows->end());
    if (!activeRows->hasSelections()) {
      // No nulls left.
      return;
    }
  }
}

// static
template <bool areSimpleInputs>
TypePtr CoalesceExpr<areSimpleInputs>::resolveType(
    const std::vector<TypePtr>& argTypes) {
  BOLT_CHECK_GT(
      argTypes.size(),
      0,
      "COALESCE statements expect to receive at least 1 argument, but did not receive any.");
  for (auto i = 1; i < argTypes.size(); i++) {
    BOLT_USER_CHECK(
        *argTypes[0] == *argTypes[i],
        "Inputs to coalesce must have the same type. Expected {}, but got {}.",
        argTypes[0]->toString(),
        argTypes[i]->toString());
  }

  return argTypes[0];
}

TypePtr CoalesceCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& argTypes) {
  return CoalesceExpr<true>::resolveType(argTypes);
}

ExprPtr CoalesceCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool /* trackCpuUsage */,
    const core::QueryConfig& /*config*/) {
  bool inputsSupportFlatNoNullsFastPath =
      Expr::allSupportFlatNoNullsFastPath(compiledChildren);
  // all inputs are constant or field reference
  auto areSimpleInputs = std::all_of(
      compiledChildren.begin(),
      compiledChildren.end(),
      [](const ExprPtr& expr) {
        return expr->isDeterministic() &&
            (expr->isConstant() || expr->is<FieldReference>());
      });
  if (areSimpleInputs) {
    return std::make_shared<CoalesceExpr<true>>(
        type, std::move(compiledChildren), inputsSupportFlatNoNullsFastPath);
  }
  return std::make_shared<CoalesceExpr<false>>(
      type, std::move(compiledChildren), inputsSupportFlatNoNullsFastPath);
}
} // namespace bytedance::bolt::exec
