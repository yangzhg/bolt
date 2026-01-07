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

#pragma once

#include <gtest/gtest.h>
#include <vector/tests/VectorTestUtils.h>

#include "bolt/expression/Expr.h"
#include "bolt/functions/colocate/client/Colocate.h"
#include "bolt/parse/Expressions.h"
#include "bolt/parse/ExpressionsParser.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
using namespace bytedance::bolt::functions;
namespace bytedance::bolt::functions::test {

std::string test_host = "127.0.0.1";

static ColocateFunctionMetadata metadata_ = {};

template <typename DerivedTestCase>
class ColocateFunctionTest : public testing::Test,
                             public bolt::test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    parse::registerTypeResolver();
    memory::MemoryManager::testingSetInstance({});
    initializeUDFClient();
    DerivedTestCase::registerColocateFunctionsForTest();
  }

  static void initializeUDFClient() {
    std::vector<std::string> hosts = {
        test_host, /* use "host.docker.internal" for docker to host instead  */
    };
    std::vector<int> ports = {8815};
    metadata_.manager = std::make_unique<UDFClientManager>(hosts, ports, 16);
  }

  core::TypedExprPtr makeTypedExpr(
      const std::string& text,
      const RowTypePtr& rowType) {
    parse::ParseOptions options = {true, true, function_prefix_};
    auto untyped = parse::parseExpr(text, options);
    return core::Expressions::inferTypes(untyped, rowType, execCtx_.pool());
  }

  VectorPtr evaluate(
      const core::TypedExprPtr& typedExpr,
      const RowVectorPtr& data,
      const std::optional<SelectivityVector>& rows = std::nullopt) {
    return evaluateImpl<exec::ExprSet>(typedExpr, data, rows);
  }

  // Use this directly if you don't want it to cast the returned vector.
  VectorPtr evaluate(
      const std::string& expression,
      const RowVectorPtr& data,
      const std::optional<SelectivityVector>& rows = std::nullopt) {
    auto typedExpr = makeTypedExpr(expression, asRowType(data->type()));

    return evaluate(typedExpr, data, rows);
  }

  template <typename T>
  std::shared_ptr<T> evaluate(
      const std::string& expression,
      const RowVectorPtr& data,
      const std::optional<SelectivityVector>& rows = std::nullopt,
      const TypePtr& resultType = nullptr) {
    auto result = evaluate(expression, data, rows);
    return castEvaluateResult<T>(result, expression, resultType);
  }

  VectorPtr evaluate(
      exec::ExprSet& exprSet,
      const RowVectorPtr& input,
      const std::optional<SelectivityVector>& rows = std::nullopt) {
    exec::EvalCtx context(&execCtx_, &exprSet, input.get());

    std::vector<VectorPtr> result(1);
    if (rows.has_value()) {
      exprSet.eval(*rows, context, result);
    } else {
      SelectivityVector defaultRows(input->size());
      exprSet.eval(defaultRows, context, result);
    }
    return result[0];
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{
      core::QueryCtx::create(executor_.get())};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};

 private:
  template <typename T>
  std::shared_ptr<T> castEvaluateResult(
      const VectorPtr& result,
      const std::string& expression,
      const TypePtr& expectedType = nullptr) {
    // reinterpret_cast is used for functions that return logical types
    // like DATE().
    BOLT_CHECK(result, "Expression evaluation result is null: {}", expression)
    if (expectedType != nullptr) {
      BOLT_CHECK_EQ(
          result->type()->kind(),
          expectedType->kind(),
          "Expression evaluation result is not of expected type kind: {} -> {} vector of type {}",
          expression,
          result->encoding(),
          result->type()->kindName())
      auto castedResult = std::reinterpret_pointer_cast<T>(result);
      return castedResult;
    }

    auto castedResult = std::dynamic_pointer_cast<T>(result);
    BOLT_CHECK(
        castedResult,
        "Expression evaluation result is not of expected type: {} -> {} vector of type {}",
        expression,
        result->encoding(),
        result->type()->toString())
    return castedResult;
  }

  template <typename ExprSet>
  VectorPtr evaluateImpl(
      const core::TypedExprPtr& typedExpr,
      const RowVectorPtr& data,
      const std::optional<SelectivityVector>& rows = std::nullopt) {
    ExprSet exprSet({typedExpr}, &execCtx_);
    return evaluate(exprSet, data, rows);
  }

 public:
  bolt::test::VectorMaker vectorMaker_{pool_.get()};

  std::string function_prefix_{"presto.default."};
};

} // namespace bytedance::bolt::functions::test
