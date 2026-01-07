/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "bolt/benchmarks/ExpressionBenchmarkBuilder.h"
#include "bolt/functions/sparksql/registration/Register.h"
using namespace bytedance;
using namespace bytedance::bolt;

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  functions::sparksql::registerFunctions("");
  ExpressionBenchmarkBuilder benchmarkBuilder;
  benchmarkBuilder.addBenchmarkSet("split", ROW({"c0"}, {VARCHAR()}))
      .withFuzzerOptions(
          {.vectorSize = 640000,
           .nullRatio = 0,
           .stringLength = 400,
           .genTimestampString = true})
      //.addExpression("split", "(split(c0,',',-1))")
      .addExpression(
          "split",
          "array_sort(split(c0,',',-1))",
          {.parseIntegerAsBigint = false})
      .withIterations(50);

  benchmarkBuilder.registerBenchmarks();
  folly::runBenchmarks();
  return 0;
}
