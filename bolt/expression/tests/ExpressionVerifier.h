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

#include "bolt/core/ITypedExpr.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/expression/fuzzer/FuzzerToolkit.h"
#include "bolt/functions/FunctionRegistry.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
namespace bytedance::bolt::test {

struct ExpressionVerifierOptions {
  bool disableConstantFolding{false};
  std::string reproPersistPath;
  bool persistAndRunOnce{false};
};

class ExpressionVerifier {
 public:
  // File names used to persist data required for reproducing a failed test
  // case.
  static constexpr const std::string_view kInputVectorFileName = "input_vector";
  static constexpr const std::string_view kIndicesOfLazyColumnsFileName =
      "indices_of_lazy_columns";
  static constexpr const std::string_view kResultVectorFileName =
      "result_vector";
  static constexpr const std::string_view kExpressionSqlFileName = "sql";
  static constexpr const std::string_view kComplexConstantsFileName =
      "complex_constants";

  ExpressionVerifier(
      core::ExecCtx* FOLLY_NONNULL execCtx,
      ExpressionVerifierOptions options)
      : execCtx_(execCtx), options_(options) {}

  // Executes expressions both using common path (all evaluation
  // optimizations) and simplified path. Additionally, a list of column
  // indices can be passed via 'columnsToWrapInLazy' which specify the
  // columns/children in the input row vector that should be wrapped in a lazy
  // layer before running it through the common evaluation path. The list can
  // contain negative column indices that represent lazy vectors that should be
  // preloaded before being fed to the evaluator. This list is sorted on the
  // absolute value of the entries.
  // Returns:
  //  - result of evaluating the expressions if both paths succeeded and
  //  returned the exact same vectors.
  //  - exception thrown by the common path if both paths failed with compatible
  //  exceptions.
  //  - throws otherwise (incompatible exceptions or different results).
  fuzzer::ResultOrError verify(
      const std::vector<core::TypedExprPtr>& plans,
      const RowVectorPtr& rowVector,
      VectorPtr&& resultVector,
      bool canThrow,
      std::vector<int> columnsToWarpInLazy = {});

 private:
  // Utility method used to serialize the relevant data required to repro a
  // crash.
  void persistReproInfo(
      const VectorPtr& inputVector,
      std::vector<int> columnsToWarpInLazy,
      const VectorPtr& resultVector,
      const std::string& sql,
      const std::vector<VectorPtr>& complexConstants);

  // Utility method that calls persistReproInfo to save data and sql if
  // options_.reproPersistPath is set and is not persistAndRunOnce. Do nothing
  // otherwise.
  void persistReproInfoIfNeeded(
      const VectorPtr& inputVector,
      const std::vector<int>& columnsToWarpInLazy,
      const VectorPtr& resultVector,
      const std::string& sql,
      const std::vector<VectorPtr>& complexConstants);

 private:
  core::ExecCtx* FOLLY_NONNULL execCtx_;
  const ExpressionVerifierOptions options_;
};

// Finds the minimum common subexpression which fails for a plan should it
// exist.
void computeMinimumSubExpression(
    ExpressionVerifier&& minimalVerifier,
    VectorFuzzer& fuzzer,
    const std::vector<core::TypedExprPtr>& plans,
    const RowVectorPtr& rowVector,
    const std::vector<int>& columnsToWrapInLazy);
} // namespace bytedance::bolt::test
