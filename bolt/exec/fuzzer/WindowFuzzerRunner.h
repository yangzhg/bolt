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

#include <folly/String.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <string>
#include <unordered_set>
#include <vector>

#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/fuzzer/AggregationFuzzerOptions.h"
#include "bolt/exec/fuzzer/WindowFuzzer.h"
#include "bolt/expression/fuzzer/FuzzerToolkit.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/serializers/PrestoSerializer.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
namespace bytedance::bolt::exec::test {

class WindowFuzzerRunner {
 public:
  static int run(
      size_t seed,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner,
      const AggregationFuzzerOptions& options) {
    return runFuzzer(
        seed, std::nullopt, std::move(referenceQueryRunner), options);
  }

  static int runRepro(
      const std::optional<std::string>& planPath,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner) {
    return runFuzzer(0, planPath, std::move(referenceQueryRunner), {});
  }

 protected:
  static int runFuzzer(
      size_t seed,
      const std::optional<std::string>& planPath,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner,
      const AggregationFuzzerOptions& options) {
    auto aggregationSignatures =
        bytedance::bolt::exec::getAggregateFunctionSignatures();
    auto windowSignatures = bytedance::bolt::exec::windowFunctions();
    if (aggregationSignatures.empty() && windowSignatures.empty()) {
      LOG(ERROR) << "No function registered.";
      exit(1);
    }

    auto filteredAggregationSignatures = bolt::fuzzer::filterSignatures(
        aggregationSignatures, options.onlyFunctions, options.skipFunctions);
    auto filteredWindowSignatures = bolt::fuzzer::filterSignatures(
        windowSignatures, options.onlyFunctions, options.skipFunctions);
    if (filteredAggregationSignatures.empty() &&
        filteredWindowSignatures.empty()) {
      LOG(ERROR)
          << "No function left after filtering using 'only' and 'skip' lists.";
      exit(1);
    }

    bytedance::bolt::parse::registerTypeResolver();
    bytedance::bolt::serializer::presto::PrestoVectorSerde::
        registerVectorSerde();
    bytedance::bolt::filesystems::registerLocalFileSystem();

    bytedance::bolt::exec::test::windowFuzzer(
        filteredAggregationSignatures,
        filteredWindowSignatures,
        seed,
        options.customVerificationFunctions,
        options.customInputGenerators,
        options.orderDependentFunctions,
        options.timestampPrecision,
        options.queryConfigs,
        planPath,
        std::move(referenceQueryRunner));
    // Calling gtest here so that it can be recognized as tests in CI systems.
    return RUN_ALL_TESTS();
  }
};

} // namespace bytedance::bolt::exec::test
