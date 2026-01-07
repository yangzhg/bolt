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

#include "bolt/benchmarks/ExpressionBenchmarkBuilder.h"
#include "bolt/functions/lib/Re2Functions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/functions/sparksql/ICURegexFunctions.h"
#include "bolt/functions/sparksql/RegexFunctions.h"
using namespace bytedance;
using namespace bytedance::bolt;
using namespace bytedance::bolt::functions;
using namespace bytedance::bolt::functions::sparksql;
using namespace bytedance::bolt::functions::test;
using namespace bytedance::bolt::memory;
using namespace bytedance::bolt;

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  exec::registerStatefulVectorFunction("like", likeSignatures(), makeLike);
  exec::registerStatefulVectorFunction(
      "icu_rlike", icuRegexLikeSignatures(), makeICURegexLike);
  exec::registerStatefulVectorFunction(
      "re2_rlike", re2SearchSignatures(), makeRLike);
  // Register the scalar functions.
  prestosql::registerAllScalarFunctions("");

  // exec::register
  ExpressionBenchmarkBuilder benchmarkBuilder;
  const vector_size_t vectorSize = 1000;
  auto vectorMaker = benchmarkBuilder.vectorMaker();

  auto makeInput =
      [&](vector_size_t vectorSize, bool padAtHead, bool padAtTail) {
        return vectorMaker.flatVector<std::string>(vectorSize, [&](auto row) {
          // Strings in even rows contain/start with/end with a_b_c depends on
          // value of padAtHead && padAtTail.
          if (row % 2 == 0) {
            auto padding = std::string(row / 2 + 1, 'x');
            if (padAtHead && padAtTail) {
              return fmt::format("{}a_b_c{}", padding, padding);
            } else if (padAtHead) {
              return fmt::format("{}a_b_c", padding);
            } else if (padAtTail) {
              return fmt::format("a_b_c{}", padding);
            } else {
              return std::string("a_b_c");
            }
          } else {
            return std::string(row, 'x');
          }
        });
      };

  auto substringInput = makeInput(vectorSize, true, true);
  auto prefixInput = makeInput(vectorSize, false, true);
  auto suffixInput = makeInput(vectorSize, true, false);

  benchmarkBuilder
      .addBenchmarkSet(
          "like_substring", vectorMaker.rowVector({"col0"}, {substringInput}))
      .addExpression("like_substring", R"(like(col0, '%a\_b\_c%', '\'))")
      .addExpression("strpos", R"(strpos(col0, 'a_b_c') > 0)");

  benchmarkBuilder
      .addBenchmarkSet(
          "like_prefix", vectorMaker.rowVector({"col0"}, {prefixInput}))
      .addExpression("like_prefix", R"(like(col0, 'a\_b\_c%', '\'))")
      .addExpression("starts_with", R"(starts_with(col0, 'a_b_c'))");

  benchmarkBuilder
      .addBenchmarkSet(
          "like_suffix", vectorMaker.rowVector({"col0"}, {suffixInput}))
      .addExpression("like_suffix", R"(like(col0, '%a\_b\_c', '\'))")
      .addExpression("ends_with", R"(ends_with(col0, 'a_b_c'))");

  benchmarkBuilder
      .addBenchmarkSet(
          "like_generic", vectorMaker.rowVector({"col0"}, {substringInput}))
      .addExpression("like_generic", R"(like(col0, 'a_b_c'))");

  benchmarkBuilder
      .addBenchmarkSet(
          "like_re2", vectorMaker.rowVector({"col0"}, {substringInput}))
      .addExpression("re2_rlike", R"(re2_rlike(col0, 'a_b_c'))");

  benchmarkBuilder
      .addBenchmarkSet(
          "like_icu", vectorMaker.rowVector({"col0"}, {substringInput}))
      .addExpression("icu_rlike", R"(icu_rlike(col0, 'a_b_c'))");

  benchmarkBuilder.registerBenchmarks();
  benchmarkBuilder.testBenchmarks();
  folly::runBenchmarks();
  return 0;
}
