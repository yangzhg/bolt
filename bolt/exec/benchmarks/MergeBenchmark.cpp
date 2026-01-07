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

#include <gflags/gflags.h>

#include "bolt/exec/TreeOfLosers.h"
#include "bolt/exec/tests/utils/MergeTestBase.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec::test;

TestData narrow;
TestData medium;
TestData wide;

BENCHMARK(narrowTree) {
  MergeTestBase::test<TreeOfLosers<TestingStream>>(narrow, false);
}

BENCHMARK_RELATIVE(narrowArray) {
  MergeTestBase::test<MergeArray<TestingStream>>(narrow, false);
}

BENCHMARK(mediumTree) {
  MergeTestBase::test<TreeOfLosers<TestingStream>>(medium, false);
}

BENCHMARK_RELATIVE(mediumArray) {
  MergeTestBase::test<MergeArray<TestingStream>>(medium, false);
}

BENCHMARK(wideTree) {
  MergeTestBase::test<TreeOfLosers<TestingStream>>(wide, false);
}

BENCHMARK_RELATIVE(wideArray) {
  MergeTestBase::test<MergeArray<TestingStream>>(wide, false);
}

int main(int argc, char* argv[]) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  MergeTestBase test;
  test.seed(1);
  narrow = test.makeTestData(100'000'000, 7);
  medium = test.makeTestData(10'000'0000, 37);
  wide = test.makeTestData(10'000'0000, 1029);
  folly::runBenchmarks();
  return 0;
}
