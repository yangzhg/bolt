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

#include "bolt/functions/prestosql/benchmarks/ArrayMinMaxBenchmark.h"

#include "bolt/benchmarks/ExpressionBenchmarkBuilder.h"
#include "bolt/common/base/BoltException.h"
#include "bolt/functions/Registerer.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
namespace bytedance::bolt::functions {

void registerTestVectorFunctionBasic() {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_array_min_basic, "vector_basic");
}

void registerTestSimpleFunctions() {
  registerFunction<ArrayMinSimpleFunction, int32_t, Array<int32_t>>(
      {"array_min_simple"});
  registerFunction<ArrayMinSimpleFunctionIterator, int32_t, Array<int32_t>>(
      {"array_min_simple_iterator"});

  registerFunction<
      ArrayMinSimpleFunctionSkipNullIterator,
      int32_t,
      Array<int32_t>>({"array_min_simple_skip_null_iterator"});
}
} // namespace bytedance::bolt::functions
using namespace bytedance::bolt;

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  functions::prestosql::registerArrayFunctions();

  functions::registerTestVectorFunctionBasic();

  functions::registerTestSimpleFunctions();
  ExpressionBenchmarkBuilder benchmarkBuilder;
  auto inputType = ROW({"c0"}, {ARRAY(INTEGER())});

  benchmarkBuilder.addBenchmarkSet("array_min_max", inputType)
      .withIterations(1000)
      .addExpression("vector_basic", "vector_basic(c0)")
      .addExpression(
          "simple_skip_null_iterator",
          "array_min_simple_skip_null_iterator(c0)")
      .addExpression("simple_iterator", "array_min_simple_iterator(c0)")
      .addExpression("simple", "array_min_simple(c0)")
      .addExpression("prestoSQLArrayMin", "array_min(c0)");

  benchmarkBuilder.registerBenchmarks();

  // Make sure all expressions within benchmarkSets have the same results.
  benchmarkBuilder.testBenchmarks();

  folly::runBenchmarks();

  return 0;
}
