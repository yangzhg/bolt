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

#include "bolt/expression/VectorFunction.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
namespace bytedance::bolt::test {

/// Taking a flat or constant array vector, wraps its element vector in a
/// dictionary that reverses the order of rows.
class TestingDictionaryArrayElementsFunction : public exec::VectorFunction {
 public:
  TestingDictionaryArrayElementsFunction() {}

  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& /*rows*/,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    ArrayVector* array{nullptr};
    if (args[0]->encoding() == VectorEncoding::Simple::ARRAY) {
      array = args[0]->as<ArrayVector>();
    } else if (args[0]->isConstantEncoding()) {
      array = args[0]
                  ->as<ConstantVector<bolt::ComplexType>>()
                  ->valueVector()
                  ->as<ArrayVector>();
    }
    BOLT_CHECK_NOT_NULL(
        array, "Input must be a flat or constant array vector.");

    auto elements = array->elements();
    const auto size = elements->size();
    auto indices = makeIndicesInReverse(size, context.pool());

    result = std::make_shared<ArrayVector>(
        context.pool(),
        array->type(),
        nullptr,
        array->size(),
        array->offsets(),
        array->sizes(),
        BaseVector::wrapInDictionary(nullptr, indices, size, elements),
        0);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // array(T) -> array(T)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("array(T)")
                .argumentType("array(T)")
                .build()};
  }
};

} // namespace bytedance::bolt::test
