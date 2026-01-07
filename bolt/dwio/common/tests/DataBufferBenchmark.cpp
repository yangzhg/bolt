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

#include "bolt/dwio/common/ChainedBuffer.h"
#include "folly/init/Init.h"
using namespace bytedance::bolt::dwio;
using namespace bytedance::bolt::dwio::common;

BENCHMARK(DataBufferOps, iters) {
  auto pool = bytedance::bolt::memory::memoryManager()->addLeafPool();
  constexpr size_t size = 1024 * 1024 * 16;
  for (size_t i = 0; i < iters; ++i) {
    DataBuffer<int32_t> buf{*pool};
    buf.reserve(size);
    for (size_t j = 0; j < size; ++j) {
      buf.unsafeAppend(j);
    }
    for (size_t j = 0; j < size; ++j) {
      folly::doNotOptimizeAway(buf[j]);
    }
  }
}

BENCHMARK(ChainedBufferOps, iters) {
  auto pool = bytedance::bolt::memory::memoryManager()->addLeafPool();
  constexpr size_t size = 1024 * 1024 * 16;
  for (size_t i = 0; i < iters; ++i) {
    ChainedBuffer<int32_t> buf{*pool, size, size * 4};
    for (size_t j = 0; j < size; ++j) {
      buf.unsafeAppend(j);
    }
    for (size_t j = 0; j < size; ++j) {
      folly::doNotOptimizeAway(buf[j]);
    }
  }
}

int main(int argc, char* argv[]) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  bytedance::bolt::memory::MemoryManager::initialize(
      bytedance::bolt::memory::MemoryManager::Options{});
  return 0;
}
