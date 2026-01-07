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

#include "bolt/functions/Macros.h"
#include "bolt/functions/Registerer.h"
#include "bolt/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;

namespace {

class RowFunctionBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  RowFunctionBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerAllScalarFunctions();
  }

  void run(const std::string& expr) {
    folly::BenchmarkSuspender suspender;
    vector_size_t size = 1'000;

    auto rowVector = vectorMaker_.rowVector({
        vectorMaker_.flatVector<int64_t>(size, [](auto row) { return row; }),
        vectorMaker_.flatVector<double>(
            size, [](auto row) { return row * 0.1; }),
    });

    auto exprSet = compileExpression(expr, rowVector->type());
    suspender.dismiss();

    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

BENCHMARK(noCopy) {
  RowFunctionBenchmark benchmark;
  benchmark.run("row_constructor(c0, c1)");
}

BENCHMARK(copyMostlyFlat) {
  RowFunctionBenchmark benchmark;
  benchmark.run(
      "if(c0 > 100, row_constructor(c0, c1), row_constructor(1, 0.1))");
}

BENCHMARK(copyMostlyConst) {
  RowFunctionBenchmark benchmark;
  benchmark.run(
      "if(c0 < 100, row_constructor(c0, c1), row_constructor(1, 0.1))");
}

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  folly::runBenchmarks();
  return 0;
}
