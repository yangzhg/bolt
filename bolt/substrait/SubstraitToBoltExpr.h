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

#pragma once

#include <fmt/format.h>
#include "bolt/core/Expressions.h"
#include "bolt/functions/FunctionRegistry.h"
#include "bolt/substrait/SubstraitParser.h"
#include "bolt/vector/ComplexVector.h"
namespace bytedance::bolt::substrait {

/// This class is used to convert Substrait representations to Bolt
/// expressions.
class SubstraitBoltExprConverter {
 public:
  /// subParser: A Substrait parser used to convert Substrait representations
  /// into recognizable representations. functionMap: A pre-constructed map
  /// storing the relations between the function id and the function name.
  explicit SubstraitBoltExprConverter(
      memory::MemoryPool* pool,
      const std::unordered_map<uint64_t, std::string>& functionMap)
      : pool_(pool),
        functionMap_(functionMap),
        functionSignatureMap_(getFunctionSignatures()) {}

  /// Convert Substrait Field into Bolt Field Expression.
  std::shared_ptr<const core::FieldAccessTypedExpr> toBoltExpr(
      const ::substrait::Expression::FieldReference& substraitField,
      const RowTypePtr& inputType);

  /// Convert Substrait ScalarFunction into Bolt Expression.
  core::TypedExprPtr toBoltExpr(
      const ::substrait::Expression::ScalarFunction& substraitFunc,
      const RowTypePtr& inputType);

  /// Convert Substrait CastExpression to Bolt Expression.
  core::TypedExprPtr toBoltExpr(
      const ::substrait::Expression::Cast& castExpr,
      const RowTypePtr& inputType);

  /// Convert Substrait Literal into Bolt Expression.
  std::shared_ptr<const core::ConstantTypedExpr> toBoltExpr(
      const ::substrait::Expression::Literal& substraitLit);

  /// Convert Substrait Expression into Bolt Expression.
  core::TypedExprPtr toBoltExpr(
      const ::substrait::Expression& substraitExpr,
      const RowTypePtr& inputType);

  /// Convert Substrait IfThen into Bolt Expression.
  core::TypedExprPtr toBoltExpr(
      const ::substrait::Expression::IfThen& substraitIfThen,
      const RowTypePtr& inputType);

 private:
  /// Convert list literal to ArrayVector.
  ArrayVectorPtr literalsToArrayVector(
      const ::substrait::Expression::Literal& listLiteral);

  /// Memory pool.
  memory::MemoryPool* pool_;

  /// The Substrait parser used to convert Substrait representations into
  /// recognizable representations.
  SubstraitParser substraitParser_;

  /// The map storing the relations between the function id and the function
  /// name.
  std::unordered_map<uint64_t, std::string> functionMap_;

  /// The map storing the relations between the function name and the function
  /// signature.
  FunctionSignatureMap functionSignatureMap_;

  /// The set storing the special functions.
  static std::unordered_set<std::string> specialFunctions_;
};

} // namespace bytedance::bolt::substrait

template <>
struct fmt::formatter<substrait::Expression::RexTypeCase> : formatter<int> {
  auto format(
      const substrait::Expression::RexTypeCase& s,
      format_context& ctx) {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};

template <>
struct fmt::formatter<substrait::Expression::Cast::FailureBehavior>
    : formatter<int> {
  auto format(
      const substrait::Expression::Cast::FailureBehavior& s,
      format_context& ctx) {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
template <>
struct fmt::formatter<substrait::Expression_FieldReference::ReferenceTypeCase>
    : formatter<int> {
  auto format(
      const substrait::Expression_FieldReference::ReferenceTypeCase& s,
      format_context& ctx) {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};

template <>
struct fmt::formatter<substrait::Expression_Literal::LiteralTypeCase>
    : formatter<int> {
  auto format(
      const substrait::Expression_Literal::LiteralTypeCase& s,
      format_context& ctx) {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
