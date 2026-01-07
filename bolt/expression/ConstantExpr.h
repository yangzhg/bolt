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

#include "bolt/expression/SpecialForm.h"
namespace bytedance::bolt::exec {
class ConstantExpr : public SpecialForm {
 public:
  explicit ConstantExpr(VectorPtr value)
      : SpecialForm(
            value->type(),
            std::vector<ExprPtr>(),
            "literal",
            !value->isNullAt(0) /* supportsFlatNoNullsFastPath */,
            false /* trackCpuUsage */),
        needToSetIsAscii_{value->type()->isVarchar()} {
    BOLT_CHECK_EQ(value->encoding(), VectorEncoding::Simple::CONSTANT);
    // sharedConstantValue_ may be modified so we should take our own copy to
    // prevent sharing across threads.
    sharedConstantValue_ =
        BaseVector::wrapInConstant(1, 0, std::move(value), true);
  }

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  void evalSpecialFormSimplified(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  const VectorPtr& value() const {
    return sharedConstantValue_;
  }

  VectorPtr& mutableValue() {
    return sharedConstantValue_;
  }

  std::string toString(bool recursive = true) const override;

  std::string toSql(
      std::vector<VectorPtr>* complexConstants = nullptr) const override;

 private:
  VectorPtr sharedConstantValue_;
  bool needToSetIsAscii_;
};
} // namespace bytedance::bolt::exec
