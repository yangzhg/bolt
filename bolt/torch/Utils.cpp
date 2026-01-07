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

#include <fmt/format.h>

#include <torch/jit.h>

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/caching/SimpleLRUCache.h"
#include "bolt/torch/Utils.h"

using ::bytedance::bolt::BoltUserError;
using ::bytedance::bolt::SimpleLRUCache;
using ::torch::jit::CompilationUnit;
using ::torch::jit::Function;

namespace {
void checkCompiledScript(const CompilationUnit& compilation_unit) {
  auto& fn = *bytedance::bolt::torch::get_function(compilation_unit);
  // The torch script function must take only one input and yield one output.
  {
    if (fn.num_inputs() != 1) {
      BOLT_FAIL(fmt::format(
          "Torch script function {} must take exactly one input. (num inputs: {})",
          fn.name(),
          fn.num_inputs()));
    }

    const ::c10::FunctionSchema& fn_schema = fn.getSchema();
    const std::vector<::c10::Argument>& returns = fn_schema.returns();
    if (returns.size() != 1) {
      BOLT_FAIL(fmt::format(
          "Torch script function {} must return exactly one input. (num outputs: {})",
          fn.name(),
          returns.size()));
    }
  }
}

} // namespace

namespace bytedance::bolt::torch {
static SimpleLRUCache<std::size_t, std::shared_ptr<CompilationUnit>> cache(512);

/// Load an existing module instance from the singleton instance of the module
/// cache if it exists. Otherwise, compile the script and store the associated
/// module in the cache.
std::shared_ptr<CompilationUnit> load(const std::string& script) {
  auto key = std::hash<std::string>{}(script);
  auto mod = cache.get(key);
  if (mod.has_value()) {
    return std::move(*mod);
  }
  try {
    auto cu = ::torch::jit::compile(script);
    checkCompiledScript(*cu);

    cache.add(key, cu);
    return cu;
  } catch (...) {
    throw BoltUserError(
        std::current_exception(),
        fmt::format("Failed to compile torch script: {}", script),
        false);
  }
}

Function* get_function(const CompilationUnit& compilation_unit) {
  auto functions = compilation_unit.get_functions();
  if (functions.empty()) {
    BOLT_FAIL("torch compilation unit does not contain a functions");
  }

  return functions.back();
}
} // namespace bytedance::bolt::torch
