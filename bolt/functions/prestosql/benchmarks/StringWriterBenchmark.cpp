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
#include <string>

#include "bolt/expression/VectorReaders.h"
#include "bolt/functions/Registerer.h"
#include "bolt/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "bolt/type/StringView.h"
namespace bytedance::bolt::exec {

namespace {

// The StringView has an optimization. If the string is small, then it is
// inlined(copied) on the stack within the StringView, instead of being
// referred to through a pointer.
// We still do write to the buffer in the current string writer always
// regardless of the string size, although it's not required in that case since
// the StringView have the date within its object memory.

template <typename T>
struct NotInlineStringFunc {
  template <typename OutT>
  void call(OutT& out, const int64_t& n) {
    out.append("Welcome to Bolt 2022!");
    out.append(std::to_string(n));
  }
};

template <typename T>
struct InlineStringFunc {
  template <typename OutT>
  void call(OutT& out, const int64_t& n) {
    out.append(std::to_string(n));
  }
};

template <typename T>
struct ArrayOfStringFunc {
  template <typename OutT>
  void call(OutT& out, const int64_t& n) {
    for (auto i = 0; i < n % 30; i++) {
      auto& item = out.add_item();
      item.append("welcome to the Bolt!!");
    }
  }
};

class StringWriterBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  StringWriterBenchmark() : FunctionBenchmarkBase() {
    registerFunction<NotInlineStringFunc, Varchar, int64_t>(
        {"not_inline_string"});

    registerFunction<InlineStringFunc, Varchar, int64_t>({"inline_string"});

    registerFunction<ArrayOfStringFunc, Array<Varchar>, int64_t>(
        {"array_of_string"});
  }

  vector_size_t size = 1000;

  auto makeInput() {
    std::vector<int64_t> inputData(size, 0);
    for (auto i = 0; i < size; i++) {
      inputData[i] = i;
    }

    auto input = vectorMaker_.rowVector({vectorMaker_.flatVector(inputData)});
    return input;
  }

  size_t run(const std::string& functionName, size_t n) {
    folly::BenchmarkSuspender suspender;
    auto input = makeInput();
    auto exprSet =
        compileExpression(fmt::format("{}(c0)", functionName), input->type());
    suspender.dismiss();
    return doRun(exprSet, input, n);
  }

  size_t doRun(ExprSet& exprSet, const RowVectorPtr& rowVector, size_t n) {
    int cnt = 0;
    for (auto i = 0; i < n; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    return cnt;
  }

  void test() {
    auto input = makeInput();

    // test not_inline_string
    {
      auto exprSet = compileExpression("not_inline_string(c0)", input->type());
      auto result = evaluate(exprSet, input);
      BOLT_CHECK_EQ(result->size(), input->size(), "size test failed");

      auto* flatResult = result->asFlatVector<StringView>();
      for (auto i = 0; i < input->size(); i++) {
        std::string expected = "Welcome to Bolt 2022!" + std::to_string(i);
        BOLT_CHECK_EQ(
            flatResult->valueAt(i), StringView(expected), "test failed");
      }
    }

    // test inline_string
    {
      auto exprSet = compileExpression("inline_string(c0)", input->type());
      auto result = evaluate(exprSet, input);
      BOLT_CHECK_EQ(result->size(), input->size(), "size test failed");

      auto* flatResult = result->asFlatVector<StringView>();
      for (auto i = 0; i < input->size(); i++) {
        std::string expected = std::to_string(i);
        BOLT_CHECK_EQ(
            flatResult->valueAt(i), StringView(expected), "test failed");
      }
    }

    // test array_of_string
    {
      auto exprSet = compileExpression("array_of_string(c0)", input->type());
      auto result = evaluate(exprSet, input);
      BOLT_CHECK_EQ(result->size(), input->size(), "size test failed");

      DecodedVector decoded;
      SelectivityVector rows(result->size());
      decoded.decode(*result.get(), rows);

      exec::VectorReader<Array<Varchar>> reader(&decoded);

      for (auto i = 0; i < input->size(); i++) {
        auto arrayView = reader.readNullFree(i);
        BOLT_CHECK_EQ(arrayView.size(), i % 30, "array size test failed");

        for (auto j = 0; j < i % 30; j++) {
          BOLT_CHECK_EQ(
              arrayView[j], "welcome to the Bolt!!"_sv, "size test failed");
        }
      }
    }
  }
};

BENCHMARK_MULTI(not_inline_string) {
  StringWriterBenchmark benchmark;
  return benchmark.run("not_inline_string", 100);
}

BENCHMARK_MULTI(inline_string) {
  StringWriterBenchmark benchmark;
  return benchmark.run("inline_string", 100);
}

BENCHMARK_MULTI(array_of_string) {
  StringWriterBenchmark benchmark;
  return benchmark.run("array_of_string", 100);
}

} // namespace
} // namespace bytedance::bolt::exec

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  bytedance::bolt::exec::StringWriterBenchmark benchmark;
  benchmark.test();
  folly::runBenchmarks();
  return 0;
}
