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

#include "bolt/functions/colocate/client/Colocate.h"
#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <folly/stop_watch.h>
#include <chrono>
#include <utility>
#include "bolt/common/base/Counters.h"
#include "bolt/common/base/StatsReporter.h"
#include "bolt/functions/colocate/client/ColocateUtilities.h"
#include "bolt/functions/colocate/client/Exception.h"
namespace bytedance::bolt::functions {

class ColocateFunction : public exec::VectorFunction {
 public:
  ColocateFunction(
      const std::string& functionName,
      const std::vector<exec::VectorFunctionArg>& inputArgs,
      ColocateFunctionMetadata& metadata)
      : metadata_(metadata) {
    std::vector<TypePtr> types;
    types.reserve(inputArgs.size());

    for (const auto& arg : inputArgs) {
      types.emplace_back(arg.type);
    }
    fullFunctionName_ = to_signature(functionName, types);
    remoteInputType_ = ROW(std::move(types));
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_USER_CHECK(
        context.execCtx()->queryCtx()->queryConfig().colocateUDFEnabled(),
        "Colocate UDF feature is disabled.");
    FORCE_RECORD_METRIC_VALUE(kUDFCall, 1);
    try {
      folly::stop_watch<milliseconds> timer;
      applyColocate(rows, args, outputType, context, result);
      auto elapsed_ms = timer.elapsed().count();
      FORCE_RECORD_METRIC_VALUE(kUDFCallTimeMs, elapsed_ms);
    } catch (const BoltRuntimeError&) {
      FORCE_RECORD_METRIC_VALUE(kUDFCallError, 1);
      throw;
    } catch (const std::exception&) {
      FORCE_RECORD_METRIC_VALUE(kUDFCallError, 1);
      context.setErrors(rows, std::current_exception());
    }
  }

 private:
  void applyColocate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      const exec::EvalCtx& context,
      VectorPtr& result) const {
    std::vector<VectorPtr> decodedVectors;
    for (auto& arg : args) {
      auto decoded = decodeSelectedVector(rows, arg);
      decodedVectors.push_back(decoded);
    }
    // Convert Bolt row vector to arrow flight vector,
    // will extract into a separate function
    auto arrowVector = boltToArrowVector(decodedVectors, context.pool());
    // Call UDFClient
    std::vector<std::string> path = {"udf", fullFunctionName_};

    auto queryContext = context.execCtx()->queryCtx();
    auto maxRetries = queryContext->queryConfig().colocateErrorRetries();
    auto timeout = queryContext->queryConfig().colocateRpcTimeout();
    auto functionCalledResult = retryCallServer(
        metadata_.manager, path, arrowVector, maxRetries, timeout);

    // Convert arrow flight vector to Bolt row vector
    arrowVectorsToBolt(functionCalledResult, &result, context.pool());
  }
  std::string fullFunctionName_;

  ColocateFunctionMetadata& metadata_;

  RowTypePtr remoteInputType_;
};

std::shared_ptr<exec::VectorFunction> createColocateFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/,
    ColocateFunctionMetadata& metadata) {
  return std::make_unique<ColocateFunction>(name, inputArgs, metadata);
}

void registerColocateFunction(
    const std::string& name,
    const std::vector<exec::FunctionSignaturePtr>& signatures,
    ColocateFunctionMetadata& metadata,
    bool overwrite) {
  exec::registerStatefulVectorFunction(
      name,
      signatures,
      std::bind(
          createColocateFunction,
          std::placeholders::_1,
          std::placeholders::_2,
          std::placeholders::_3,
          std::ref(metadata)),
      std::ref(metadata),
      overwrite);
}
} // namespace bytedance::bolt::functions
