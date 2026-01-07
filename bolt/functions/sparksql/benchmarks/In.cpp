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

#include <string>

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/Conv.h>
#include <folly/Random.h>
#include <folly/init/Init.h>

#include "bolt/functions/lib/Re2Functions.h"
#include "bolt/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "bolt/functions/sparksql/In.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
namespace bytedance::bolt::functions {
void registerPrestoIn() {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_in, "presto");
}
void registerArrayConstructor() {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_array_constructor, "array_constructor");
}
} // namespace bytedance::bolt::functions
namespace bytedance::bolt::functions::sparksql {
namespace {

int in_int(int iters, int inListSize, const std::string& functionName) {
  folly::BenchmarkSuspender kSuspender;
  test::FunctionBenchmarkBase benchmarkBase;

  VectorFuzzer::Options opts;
  opts.vectorSize = 1024;
  auto vector = VectorFuzzer(opts, benchmarkBase.pool()).fuzzFlat(BIGINT());
  auto simpleVector = vector->as<SimpleVector<int64_t>>();
  const auto data = benchmarkBase.maker().rowVector({vector});

  std::string exprStr = functionName + "(c0, array_constructor(";
  for (int i = 0; i < inListSize; i++) {
    if (i > 0) {
      exprStr += ", ";
    }
    exprStr += fmt::format(
        "{}", simpleVector->valueAt(folly::Random::rand32() % opts.vectorSize));
  }
  exprStr += "))";
  exec::ExprSet expr = benchmarkBase.compileExpression(exprStr, data->type());
  kSuspender.dismiss();
  for (int i = 0; i != iters; ++i) {
    benchmarkBase.evaluate(expr, data);
  }
  return iters * opts.vectorSize;
}

BENCHMARK_NAMED_PARAM_MULTI(in_int, presto_rhs1, 1, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_int, spark_rhs1, 1, "sparkin");
BENCHMARK_NAMED_PARAM_MULTI(in_int, presto_rhs3, 3, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_int, spark_rhs3, 3, "sparkin");
BENCHMARK_NAMED_PARAM_MULTI(in_int, presto_rhs10, 10, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_int, spark_rhs10, 10, "sparkin");
BENCHMARK_NAMED_PARAM_MULTI(in_int, presto_rhs100, 100, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_int, spark_rhs100, 100, "sparkin");
BENCHMARK_NAMED_PARAM_MULTI(in_int, presto_rhs1000, 1000, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_int, spark_rhs1000, 1000, "sparkin");

int in_str(int iters, int inListSize, const std::string& functionName) {
  folly::BenchmarkSuspender kSuspender;
  test::FunctionBenchmarkBase benchmarkBase;

  VectorFuzzer::Options opts;
  opts.vectorSize = 1024;
  auto vector = VectorFuzzer(opts, benchmarkBase.pool()).fuzzFlat(VARCHAR());
  auto simpleVector = vector->as<SimpleVector<StringView>>();
  const auto data = benchmarkBase.maker().rowVector({vector});

  std::string exprStr = functionName + "(c0, array_constructor(";
  for (int i = 0; i < inListSize; i++) {
    if (i > 0) {
      exprStr += ", ";
    }
    exprStr += fmt::format(
        "'{}'",
        simpleVector->valueAt(folly::Random::rand32() % opts.vectorSize));
  }
  exprStr += "))";
  exec::ExprSet expr = benchmarkBase.compileExpression(exprStr, data->type());
  kSuspender.dismiss();
  for (int i = 0; i != iters; ++i) {
    benchmarkBase.evaluate(expr, data);
  }
  return iters * opts.vectorSize;
}

BENCHMARK_NAMED_PARAM_MULTI(in_str, presto_rhs1, 1, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_str, spark_rhs1, 1, "sparkin");
BENCHMARK_NAMED_PARAM_MULTI(in_str, presto_rhs3, 3, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_str, spark_rhs3, 3, "sparkin");
BENCHMARK_NAMED_PARAM_MULTI(in_str, presto_rhs10, 10, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_str, spark_rhs10, 10, "sparkin");
BENCHMARK_NAMED_PARAM_MULTI(in_str, presto_rhs100, 100, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_str, spark_rhs100, 100, "sparkin");
BENCHMARK_NAMED_PARAM_MULTI(in_str, presto_rhs1000, 1000, "presto");
BENCHMARK_RELATIVE_NAMED_PARAM_MULTI(in_str, spark_rhs1000, 1000, "sparkin");

} // namespace

void registerInFunctions() {
  registerIn("spark");
}

} // namespace bytedance::bolt::functions::sparksql

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  bytedance::bolt::functions::registerPrestoIn();
  bytedance::bolt::functions::sparksql::registerInFunctions();
  bytedance::bolt::functions::registerArrayConstructor();
  folly::runBenchmarks();
  return 0;
}
