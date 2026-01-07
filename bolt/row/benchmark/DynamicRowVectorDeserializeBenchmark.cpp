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
#include <random>

#include "bolt/row/UnsafeRowDeserializers.h"
#include "bolt/row/UnsafeRowFast.h"
#include "bolt/type/Type.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorMaker.h"
namespace bytedance::spark::benchmarks {
namespace {
using namespace bytedance::bolt;
using namespace bytedance::bolt::row;
using bytedance::bolt::test::VectorMaker;

class Deserializer {
 public:
  virtual ~Deserializer() = default;
  virtual void deserialize(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type) = 0;
};

class UnsaferowBatchDeserializer : public Deserializer {
 public:
  UnsaferowBatchDeserializer() {}

  void deserialize(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type) override {
    UnsafeRowDeserializer::deserialize(data, type, pool_.get());
  }

 private:
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};

class BenchmarkHelper {
 public:
  std::tuple<std::vector<std::optional<std::string_view>>, TypePtr>
  randomUnsaferows(int nFields, int nRows, bool stringOnly) {
    RowTypePtr rowType;
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    names.reserve(nFields);
    types.reserve(nFields);
    for (int32_t i = 0; i < nFields; ++i) {
      names.push_back("");
      if (stringOnly) {
        types.push_back(VARCHAR());
      } else {
        auto idx = folly::Random::rand32() % allTypes_.size();
        types.push_back(allTypes_[idx]);
      }
    }
    rowType =
        TypeFactory<TypeKind::ROW>::create(std::move(names), std::move(types));

    VectorFuzzer::Options opts;
    opts.vectorSize = 1;
    opts.nullRatio = 0.1;
    opts.stringVariableLength = true;
    opts.stringLength = 20;
    // Spark uses microseconds to store timestamp
    opts.timestampPrecision =
        VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;

    auto seed = folly::Random::rand32();
    VectorFuzzer fuzzer(opts, pool_.get(), seed);
    const auto inputVector = fuzzer.fuzzInputRow(rowType);
    std::vector<std::optional<std::string_view>> results;
    results.reserve(nRows);
    // Serialize rowVector into bytes.
    UnsafeRowFast unsafeRow(inputVector);
    for (int32_t i = 0; i < nRows; ++i) {
      BufferPtr bufferPtr =
          AlignedBuffer::allocate<char>(1024, pool_.get(), true);
      char* buffer = bufferPtr->asMutable<char>();
      auto rowSize = unsafeRow.serialize(0, buffer);
      results.push_back(std::string_view(buffer, rowSize));
    }
    return {results, rowType};
  }

 private:
  std::vector<TypePtr> allTypes_{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      TIMESTAMP(),
      ARRAY(INTEGER()),
      MAP(VARCHAR(), ARRAY(INTEGER())),
      ROW({INTEGER()})};

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};

int deserialize(
    int nIters,
    int nFields,
    int nRows,
    bool variable,
    std::unique_ptr<Deserializer> deserializer) {
  folly::BenchmarkSuspender suspender;
  BenchmarkHelper helper;
  auto [data, rowType] = helper.randomUnsaferows(nFields, nRows, variable);
  suspender.dismiss();

  for (int i = 0; i < nIters; i++) {
    deserializer->deserialize(data, rowType);
  }

  return nIters * nFields * nRows;
}

BENCHMARK_NAMED_PARAM_MULTI(
    deserialize,
    batch_10_100k_string_only,
    10,
    100000,
    true,
    std::make_unique<UnsaferowBatchDeserializer>());

BENCHMARK_NAMED_PARAM_MULTI(
    deserialize,
    batch_100_100k_string_only,
    100,
    100000,
    true,
    std::make_unique<UnsaferowBatchDeserializer>());

BENCHMARK_NAMED_PARAM_MULTI(
    deserialize,
    batch_10_100k_all_types,
    10,
    100000,
    false,
    std::make_unique<UnsaferowBatchDeserializer>());

BENCHMARK_NAMED_PARAM_MULTI(
    deserialize,
    batch_100_100k_all_types,
    100,
    100000,
    false,
    std::make_unique<UnsaferowBatchDeserializer>());

} // namespace
} // namespace bytedance::spark::benchmarks

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  bytedance::bolt::memory::MemoryManager::initialize({});
  folly::runBenchmarks();
  return 0;
}
