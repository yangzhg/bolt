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
#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "bolt/functions/prestosql/Comparisons.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
namespace bytedance::bolt::functions {

void registerVectorFunctions() {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_simd_comparison_eq, "eq");
  registerBinaryScalar<EqFunction, bool>({"nonsimd_eq"});
}

} // namespace bytedance::bolt::functions

namespace {
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::functions;

class ComparisonsBechmark
    : public bytedance::bolt::functions::test::FunctionBenchmarkBase {
 public:
  ComparisonsBechmark() {
    registerVectorFunctions();
  }

  template <TypeKind kind>
  RowVectorPtr createRowData() {
    BOLT_CHECK(TypeTraits<kind>::isPrimitiveType);
    VectorFuzzer::Options opts;
    opts.nullRatio = 0;
    opts.vectorSize = 10'000;
    VectorFuzzer fuzzer(opts, execCtx_.pool());
    auto type = TypeTraits<kind>::ImplType::create();
    auto vectorLeft = fuzzer.fuzzFlat(type);
    auto vectorRight = fuzzer.fuzzFlat(type);
    return vectorMaker_.rowVector({vectorLeft, vectorRight});
  }

  template <TypeKind kind>
  void runNonSimdComparison() {
    folly::BenchmarkSuspender suspender;
    auto rowVector = createRowData<kind>();
    auto exprSet = compileExpression("nonsimd_eq(c0, c1)", rowVector->type());
    suspender.dismiss();

    doRun(exprSet, rowVector);
  }

  template <TypeKind kind>
  void runSimdComparison() {
    folly::BenchmarkSuspender suspender;
    registerVectorFunctions();
    auto rowVector = createRowData<kind>();
    auto exprSet = compileExpression("eq(c0, c1)", rowVector->type());
    suspender.dismiss();
    doRun(exprSet, rowVector);
  }

  void doRun(ExprSet& exprSet, const RowVectorPtr& rowVector) {
    uint32_t cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

BENCHMARK(non_simd_bigint_eq) {
  ComparisonsBechmark benchmark;
  benchmark.runNonSimdComparison<TypeKind::BIGINT>();
}

BENCHMARK_RELATIVE(simd_bigint_eq) {
  ComparisonsBechmark benchmark;
  benchmark.runSimdComparison<TypeKind::BIGINT>();
}

BENCHMARK(non_simd_tinyint_eq) {
  ComparisonsBechmark benchmark;
  benchmark.runNonSimdComparison<TypeKind::TINYINT>();
}

BENCHMARK_RELATIVE(simd_tinyint_eq) {
  ComparisonsBechmark benchmark;
  benchmark.runSimdComparison<TypeKind::TINYINT>();
}

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  bytedance::bolt::memory::MemoryManager::initialize(
      bytedance::bolt::memory::MemoryManager::Options{});

  folly::runBenchmarks();
  return 0;
}
