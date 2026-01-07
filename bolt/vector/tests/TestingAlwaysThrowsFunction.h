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
namespace bytedance::bolt::test {
template <typename T>
struct TestingAlwaysThrowsFunction {
  template <typename TResult, typename TInput>
  void call(TResult&, const TInput&) {
    BOLT_USER_FAIL();
  }
};

template <typename T>
struct TestingThrowsAtOddFunction {
  void call(bool& out, const int64_t& input) {
    if (input % 2) {
      BOLT_USER_FAIL();
    } else {
      out = 1;
    }
  }
};

// Throw a BoltException if boltException_ is true. Throw an std exception
// otherwise.
class TestingAlwaysThrowsVectorFunction : public exec::VectorFunction {
 public:
  static constexpr const char* kBoltErrorMessage = "Bolt Exception: Expected";
  static constexpr const char* kStdErrorMessage = "Std Exception: Expected";

  explicit TestingAlwaysThrowsVectorFunction(bool boltException)
      : boltException_{boltException} {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& /* args */,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& /* result */) const override {
    if (boltException_) {
      auto error =
          std::make_exception_ptr(std::invalid_argument(kBoltErrorMessage));
      context.setErrors(rows, error);
      return;
    }
    throw std::invalid_argument(kStdErrorMessage);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder()
                .returnType("boolean")
                .argumentType("integer")
                .build()};
  }

 private:
  const bool boltException_;
};

} // namespace bytedance::bolt::test
