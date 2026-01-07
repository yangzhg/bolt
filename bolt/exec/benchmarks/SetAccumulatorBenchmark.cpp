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

#include "bolt/common/memory/Memory.h"
#include "bolt/exec/SetAccumulator.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;

namespace {

// Adds 10M mostly unique values to a single SetAccumulator, then extracts
// unique values from it.
class SetAccumulatorBenchmark : public bytedance::bolt::test::VectorTestBase {
 public:
  void setup() {
    VectorFuzzer::Options opts;
    opts.vectorSize = 1'000'000;
    VectorFuzzer fuzzer(opts, pool());

    auto rowType = ROW({"a", "b", "c"}, {BIGINT(), BIGINT(), VARCHAR()});
    for (auto i = 0; i < 10; ++i) {
      rowVectors_.emplace_back(fuzzer.fuzzInputRow(rowType));
    }
  }

  void runBigint() {
    runPrimitive<int64_t>("a");
  }

  void runVarchar() {
    runPrimitive<StringView>("c");
  }

  void runTwoBigints() {
    HashStringAllocator allocator(pool());
    const TypePtr type = ROW({BIGINT(), BIGINT()});
    aggregate::prestosql::SetAccumulator<ComplexType> accumulator(
        type, &allocator);

    for (const auto& rowVector : rowVectors_) {
      auto vector =
          makeRowVector({rowVector->childAt("a"), rowVector->childAt("b")});
      DecodedVector decoded(*vector);
      for (auto i = 0; i < rowVector->size(); ++i) {
        accumulator.addValue(decoded, i, &allocator);
      }
    }

    auto result = BaseVector::create(type, accumulator.size(), pool());
    accumulator.extractValues(*result, 0);
    folly::doNotOptimizeAway(result);
  }

 private:
  template <typename T>
  void runPrimitive(const std::string& name) {
    const auto& type = rowVectors_[0]->childAt(name)->type();

    HashStringAllocator allocator(pool());
    aggregate::prestosql::SetAccumulator<T> accumulator(type, &allocator);

    for (const auto& rowVector : rowVectors_) {
      DecodedVector decoded(*rowVector->childAt(name));
      for (auto i = 0; i < rowVector->size(); ++i) {
        accumulator.addValue(decoded, i, &allocator);
      }
    }

    auto result =
        BaseVector::create<FlatVector<T>>(type, accumulator.size(), pool());
    accumulator.extractValues(*result, 0);
    folly::doNotOptimizeAway(result);
  }

  std::vector<RowVectorPtr> rowVectors_;
};

std::unique_ptr<SetAccumulatorBenchmark> bm;

BENCHMARK(bigint) {
  bm->runBigint();
}

BENCHMARK(varchar) {
  bm->runVarchar();
}

BENCHMARK(twoBigints) {
  bm->runTwoBigints();
}

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});

  bm = std::make_unique<SetAccumulatorBenchmark>();
  bm->setup();

  folly::runBenchmarks();

  bm.reset();

  return 0;
}
