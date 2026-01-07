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

// Assumes flat arrays with flat elements placed sequentially with no gaps. Also
// assumes arrays on the left and on the rights sides have matching sizes and
// offsets. Evaluates on all rows.
VectorPtr evaluateFast(const RowVector& data) {
  auto left = data.childAt(0)->asUnchecked<ArrayVector>();
  auto right = data.childAt(1)->asUnchecked<ArrayVector>();
  auto numElements = left->elements()->size();
  auto rawLeft =
      left->elements()->asUnchecked<FlatVector<int64_t>>()->rawValues();
  auto rawRight =
      right->elements()->asUnchecked<FlatVector<int64_t>>()->rawValues();

  auto result = BaseVector::create(BIGINT(), numElements, data.pool());
  auto flatResult = result->asUnchecked<FlatVector<int64_t>>();
  auto rawResults = flatResult->mutableRawValues();

  for (auto i = 0; i < numElements; ++i) {
    rawResults[i] = rawLeft[i] + rawRight[i];
  }

  return std::make_shared<ArrayVector>(
      left->pool(),
      left->type(),
      nullptr,
      left->size(),
      left->offsets(),
      left->sizes(),
      result);
}

class ZipWithBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  explicit ZipWithBenchmark(uint32_t seed)
      : FunctionBenchmarkBase(), seed_{seed} {
    functions::prestosql::registerAllScalarFunctions();
  }

  RowVectorPtr generateData() {
    VectorFuzzer::Options options;
    options.vectorSize = 10'024;

    VectorFuzzer fuzzer(options, pool(), seed_);

    return vectorMaker_.rowVector(
        {fuzzer.fuzzFlat(ARRAY(BIGINT())), fuzzer.fuzzFlat(ARRAY(BIGINT()))});
  }

  void test() {
    auto data = generateData();

    auto basicResult = evaluate(kBasicExpression, data);
    auto fastResult = evaluateFast(*data);

    test::assertEqualVectors(basicResult, fastResult);
  }

  size_t runBasic(size_t times) {
    folly::BenchmarkSuspender suspender;
    auto data = generateData();
    auto exprSet = compileExpression(kBasicExpression, asRowType(data->type()));
    suspender.dismiss();

    return doRun(exprSet, data, times);
  }

  size_t runFast(size_t times) {
    folly::BenchmarkSuspender suspender;
    auto data = generateData();
    suspender.dismiss();

    int cnt = 0;
    for (auto i = 0; i < times * 1'000; i++) {
      cnt += evaluateFast(*data)->size();
    }
    return cnt;
  }

 private:
  static const std::string kBasicExpression;

  size_t doRun(ExprSet& exprSet, const RowVectorPtr& rowVector, size_t times) {
    int cnt = 0;
    for (auto i = 0; i < times * 1'000; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    return cnt;
  }

  const uint32_t seed_;
};

const std::string ZipWithBenchmark::kBasicExpression =
    "zip_with(c0, c1, (x, y) -> x + y)";

const uint32_t seed = folly::Random::rand32();

BENCHMARK_MULTI(basic, n) {
  ZipWithBenchmark benchmark(seed);
  return benchmark.runBasic(n);
}

BENCHMARK_MULTI(fast, n) {
  ZipWithBenchmark benchmark(seed);
  return benchmark.runFast(n);
}

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  LOG(ERROR) << "Seed: " << seed;
  {
    ZipWithBenchmark benchmark(seed);
    benchmark.test();
  }
  folly::runBenchmarks();
  return 0;
}
