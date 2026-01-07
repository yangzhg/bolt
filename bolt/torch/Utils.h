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

#include <memory>
#include <string>

#include <torch/csrc/jit/api/compilation_unit.h>

namespace bytedance::bolt::torch {
// Load a torch script from the global cache.
// If the script does not exist in the cache, it will be compiled
// and added to the cache.
std::shared_ptr<::torch::jit::CompilationUnit> load(const std::string& script);

// Get the name of the function to use from a compiled torch script.
// This is the name of the last function in the script
::torch::jit::Function* get_function(
    const ::torch::jit::CompilationUnit& compilation_unit);
} // namespace bytedance::bolt::torch
