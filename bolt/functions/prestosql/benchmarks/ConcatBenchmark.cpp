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

#include "bolt/functions/Registerer.h"
#include "bolt/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;

namespace {

class ConcatBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit ConcatBenchmark(uint32_t seed)
      : FunctionBenchmarkBase(), seed_{seed} {
    functions::prestosql::registerStringFunctions();
  }

  RowVectorPtr generateData() {
    VectorFuzzer::Options options;
    options.vectorSize = 10'024;
    options.stringVariableLength = true;
    options.stringLength = 64;

    VectorFuzzer fuzzer(options, pool(), seed_);

    return vectorMaker_.rowVector(
        {fuzzer.fuzzFlat(VARCHAR()), fuzzer.fuzzFlat(VARCHAR())});
  }

  VectorPtr evaluateOnce(
      const std::string& expression,
      const RowVectorPtr& data) {
    auto exprSet = compileExpression(expression, asRowType(data->type()));
    return evaluate(exprSet, data);
  }

  void test() {
    auto data = generateData();

    auto basicResult = evaluateOnce(kBasicExpression, data);
    auto flattenedResult = evaluateOnce(kFlattenedExpression, data);
    auto flattenedAndConstantFoldedResult =
        evaluateOnce(kFlattenedAndConstantFoldedExpression, data);

    test::assertEqualVectors(basicResult, flattenedResult);
    test::assertEqualVectors(basicResult, flattenedAndConstantFoldedResult);
  }

  size_t runBasic(size_t times) {
    folly::BenchmarkSuspender suspender;
    auto data = generateData();
    auto exprSet = compileExpression(kBasicExpression, asRowType(data->type()));
    suspender.dismiss();

    return doRun(exprSet, data, times);
  }

  size_t runFlattened(size_t times) {
    folly::BenchmarkSuspender suspender;
    auto data = generateData();
    auto exprSet =
        compileExpression(kFlattenedExpression, asRowType(data->type()));
    suspender.dismiss();

    return doRun(exprSet, data, times);
  }

  size_t runFlattenedAndConstantFolded(size_t times) {
    folly::BenchmarkSuspender suspender;
    auto data = generateData();
    auto exprSet = compileExpression(
        kFlattenedAndConstantFoldedExpression, asRowType(data->type()));
    suspender.dismiss();

    return doRun(exprSet, data, times);
  }

  size_t doRun(ExprSet& exprSet, const RowVectorPtr& rowVector, size_t times) {
    int cnt = 0;
    for (auto i = 0; i < times * 1'000; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    return cnt;
  }

 private:
  static const std::string kBasicExpression;
  static const std::string kFlattenedExpression;
  static const std::string kFlattenedAndConstantFoldedExpression;

  const uint32_t seed_;
};

const std::string ConcatBenchmark::kBasicExpression =
    "concat(c0, concat(', ', concat(c1, concat(',', concat('567', concat(',', concat('129', concat(',', '987654321'))))))))";
const std::string ConcatBenchmark::kFlattenedExpression =
    "concat(c0, ', ', c1, ',', '567', ',', '129', ',', '987654321')";
const std::string ConcatBenchmark::kFlattenedAndConstantFoldedExpression =
    "concat(c0, ', ', c1, ',567,129,987654321')";

const uint32_t seed = folly::Random::rand32();

BENCHMARK_MULTI(basic, n) {
  ConcatBenchmark benchmark(seed);
  return benchmark.runBasic(n);
}

BENCHMARK_MULTI(flatten, n) {
  ConcatBenchmark benchmark(seed);
  return benchmark.runFlattened(n);
}

BENCHMARK_MULTI(flattenAndConstantFold, n) {
  ConcatBenchmark benchmark(seed);
  return benchmark.runFlattenedAndConstantFolded(n);
}

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  LOG(ERROR) << "Seed: " << seed;
  {
    ConcatBenchmark benchmark(seed);
    benchmark.test();
  }
  folly::runBenchmarks();
  return 0;
}
