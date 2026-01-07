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

#include "bolt/expression/EvalCtx.h"
#include "bolt/expression/Expr.h"
#include "bolt/expression/StringWriter.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt::functions::sparksql {

class InputFileNameFunction final : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& /*args*/,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    StringView split = context.currentSplitStr();
    result = BaseVector::createConstant(
        VARCHAR(), split, rows.end(), context.pool());
  }

  bool isDeterministic() const override {
    return false;
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder().returnType("varchar").build()};
  }
};

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_input_file_name,
    InputFileNameFunction::signatures(),
    std::make_unique<InputFileNameFunction>());

} // namespace bytedance::bolt::functions::sparksql
