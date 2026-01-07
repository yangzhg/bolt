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
#include <folly/container/F14Set.h>
#include <folly/init/Init.h>
#include "bolt/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::test;

namespace {

/// Fast implementation of IN (a, b, c,..) using F14FastSet.
VectorPtr fastIn(
    const folly::F14FastSet<int32_t>& inSet,
    const VectorPtr& data) {
  const auto numRows = data->size();
  auto result = std::static_pointer_cast<FlatVector<bool>>(
      BaseVector::create(BOOLEAN(), numRows, data->pool()));
  auto rawResults = result->mutableRawValues<int32_t>();

  auto rawData = data->asUnchecked<FlatVector<int32_t>>()->rawValues();
  for (auto row = 0; row < numRows; ++row) {
    bits::setBit(rawResults, row, inSet.contains(rawData[row]));
  }

  return result;
}

class InBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  InBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerGeneralFunctions();
  }

  RowVectorPtr makeData() {
    VectorFuzzer::Options opts;
    opts.vectorSize = 1'000;
    return vectorMaker_.rowVector(
        {VectorFuzzer(opts, pool()).fuzzFlat(INTEGER())});
  }

  void run(size_t numValues) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData();

    std::ostringstream inList;
    inList << "0";
    for (auto i = 1; i < numValues; ++i) {
      inList << ", " << i * 2;
    }

    auto sql = fmt::format("c0 IN ({})", inList.str());
    auto exprSet = compileExpression(sql, data->type());
    suspender.dismiss();

    doRun(exprSet, data);
  }

  void doRun(ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 1000; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }

  void runFast(size_t numValues) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData();

    folly::F14FastSet<int32_t> inSet;
    inSet.reserve(numValues);
    for (auto i = 0; i < numValues; ++i) {
      inSet.insert(i);
    }
    suspender.dismiss();

    doRunFast(inSet, data->childAt(0));
  }

  void doRunFast(
      const folly::F14FastSet<int32_t>& inSet,
      const VectorPtr& data) {
    int cnt = 0;
    for (auto i = 0; i < 1000; i++) {
      cnt += fastIn(inSet, data)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

BENCHMARK(fastIn) {
  InBenchmark benchmark;
  benchmark.runFast(10);
}

BENCHMARK_RELATIVE(in) {
  InBenchmark benchmark;
  benchmark.run(10);
}

BENCHMARK(fastIn1K) {
  InBenchmark benchmark;
  benchmark.runFast(1'000);
}

BENCHMARK_RELATIVE(in1K) {
  InBenchmark benchmark;
  benchmark.run(1'000);
}

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  folly::runBenchmarks();
  return 0;
}
