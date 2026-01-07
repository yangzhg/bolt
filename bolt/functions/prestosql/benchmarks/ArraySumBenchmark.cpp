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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "bolt/benchmarks/ExpressionBenchmarkBuilder.h"
#include "bolt/functions/lib/LambdaFunctionUtil.h"
#include "bolt/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "bolt/functions/prestosql/ArrayFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::functions;

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  functions::prestosql::registerArrayFunctions();
  registerFunction<ArraySumFunction, int64_t, Array<int32_t>>(
      {"array_sum_alt"});

  ExpressionBenchmarkBuilder benchmarkBuilder;
  auto inputType = ROW({"c0"}, {ARRAY(INTEGER())});

  auto createSet = [&](bool withNulls) {
    benchmarkBuilder
        .addBenchmarkSet(
            fmt::format("array_sum_{}", withNulls ? "nulls" : "nullfree"),
            inputType)
        .withFuzzerOptions(
            {.vectorSize = 1000, .nullRatio = withNulls ? 0.2 : 0})
        .addExpression("vector", "array_sum(c0)")
        .addExpression("simple", "array_sum_alt(c0)");
  };

  createSet(true);
  createSet(false);

  benchmarkBuilder.registerBenchmarks();

  // Make sure all expressions within benchmarkSets have the same results.
  benchmarkBuilder.testBenchmarks();

  folly::runBenchmarks();
  return 0;
}
