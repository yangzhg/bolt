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

#include "bolt/expression/CoalesceExpr.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/expression/FieldReference.h"
#include "bolt/vector/BaseVector.h"
namespace bytedance::bolt::exec {

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
