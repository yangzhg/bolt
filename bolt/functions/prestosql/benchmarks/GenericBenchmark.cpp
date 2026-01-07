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
#include "bolt/common/base/BoltException.h"
#include "bolt/expression/ComplexViewTypes.h"
#include "bolt/functions/Registerer.h"
#include "bolt/functions/Udf.h"
#include "bolt/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"
using namespace bytedance::bolt;

namespace {
// The following two functions is used to measure the cost of the cast of
// generic view.

// A function that takes array<X> and returns the summation as int64_t. For
// simplicity is here handling double and int64_t.
template <typename T>
struct GenericInputArraySum {
  template <typename TInput>
  void call(int64_t& out, const TInput& arrayOfGeneric) {
    out = 0;
    if (arrayOfGeneric.elementKind() == TypeKind::DOUBLE) {
      for (auto e : arrayOfGeneric) {
        if (e.has_value()) {
          out += e.value().template castTo<double>();
        }
      }
    } else if (arrayOfGeneric.elementKind() == TypeKind::BIGINT) {
      for (auto e : arrayOfGeneric) {
        if (e.has_value()) {
          out += e.value().template castTo<int64_t>();
        }
      }
    } else {
      out = 0;
    }
  }
};

template <typename T>
struct TypedArraySum {
  template <typename TInput>
  void call(int64_t& out, const TInput& typedArray) {
    out = 0;
    for (auto e : typedArray) {
      if (e.has_value()) {
        out += e.value();
      }
    }
  }
};

// The following two functions is used to measure the cost of the cast of
// generic writer when casted to for primitive type.
template <typename T>
struct FullGenericArraySum {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  void call(
      out_type<Generic<T1>>& outGeneric,
      const arg_type<Array<Generic<T1>>>& arrayOfGeneric) {
    if (arrayOfGeneric.elementKind() == TypeKind::DOUBLE) {
      auto& out = outGeneric.template castTo<double>();
      out = 0;
      for (auto e : arrayOfGeneric) {
        if (e.has_value()) {
          out += e.value().template castTo<double>();
        }
      }
    } else if (arrayOfGeneric.elementKind() == TypeKind::BIGINT) {
      auto& out = outGeneric.template castTo<int64_t>();
      out = 0;
      for (auto e : arrayOfGeneric) {
        if (e.has_value()) {
          out += e.value().template castTo<int64_t>();
        }
      }
    } else {
      BOLT_UNREACHABLE("not implemented");
    }
  }
};

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  ExpressionBenchmarkBuilder benchmarkBuilder;

  bytedance::bolt::
      registerFunction<FullGenericArraySum, Generic<T1>, Array<Generic<T1>>>(
          {"full_generic_sum"});
  bytedance::bolt::registerFunction<GenericInputArraySum, int64_t, Array<Any>>(
      {"generic_input_sum"});
  bytedance::bolt::registerFunction<TypedArraySum, int64_t, Array<double>>(
      {"typed_sum"});
  bytedance::bolt::registerFunction<TypedArraySum, int64_t, Array<int64_t>>(
      {"typed_sum"});

  benchmarkBuilder
      .addBenchmarkSet(
          fmt::format("array_sum"),
          ROW({"c0", "c1"}, {ARRAY(BIGINT()), ARRAY(DOUBLE())}))
      .addExpression("full_generic_int", "full_generic_sum(c0)")
      .addExpression("generic_input_int", "generic_input_sum(c0)")
      .addExpression("typed_int", "typed_sum(c0)")
      .addExpression("full_generic_double", "full_generic_sum(c1)")
      .addExpression("generic_input_double", "generic_input_sum(c1)")
      .addExpression("typed_double", "typed_sum(c1)");

  benchmarkBuilder.registerBenchmarks();

  folly::runBenchmarks();
  return 0;
}
