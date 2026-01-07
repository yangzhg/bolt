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
using namespace bytedance::bolt::test;

namespace {

template <typename T>
struct NotScalarFunction {
  bool call(bool& result, const bool& arg) {
    result = !arg;
    return true;
  }
};

class NotBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  NotBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerArithmeticFunctions();
    registerFunction<NotScalarFunction, bool, bool>({"not_scalar"});
  }

  void run(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    constexpr vector_size_t size = 1000;

    // False in even positions: False, True, False, True, .....; with some nulls
    auto evenFalse = vectorMaker_.rowVector({vectorMaker_.flatVector<bool>(
        size,
        [](auto row) { return row % 2 == 1; },
        VectorMaker::nullEvery(5))});
    auto exprSet = compileExpression(
        fmt::format("{}(c0)", functionName), evenFalse->type());
    suspender.dismiss();

    doRun(exprSet, evenFalse);
  }

  void doRun(ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

BENCHMARK(scalarNot) {
  NotBenchmark benchmark;
  benchmark.run("not_scalar");
}

BENCHMARK_RELATIVE(vectorizedNot) {
  NotBenchmark benchmark;
  benchmark.run("not");
}

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  folly::runBenchmarks();
  return 0;
}
