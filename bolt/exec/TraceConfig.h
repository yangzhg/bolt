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

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_set>
namespace bytedance::bolt::exec::trace {

#define BOLT_TRACE_LIMIT_EXCEEDED(errorMessage)                     \
  _BOLT_THROW(                                                      \
      ::bytedance::bolt::BoltRuntimeError,                          \
      ::bytedance::bolt::error_source::kErrorSourceRuntime.c_str(), \
      ::bytedance::bolt::error_code::kTraceLimitExceeded.c_str(),   \
      /* isRetriable */ true,                                       \
      "{}",                                                         \
      errorMessage);

/// The callback used to update and aggregate the trace bytes of a query. If the
/// query trace limit is set, the callback return true if the aggregate traced
/// bytes exceed the set limit otherwise return false.
using UpdateAndCheckTraceLimitCB = std::function<void(uint64_t)>;

struct TraceConfig {
  /// Target query trace nodes.
  std::unordered_set<std::string> queryNodes;
  /// Base dir of query trace.
  std::string queryTraceDir;
  UpdateAndCheckTraceLimitCB updateAndCheckTraceLimitCB;
  /// The trace task regexp.
  std::string taskRegExp;

  TraceConfig(
      std::unordered_set<std::string> _queryNodeIds,
      std::string _queryTraceDir,
      UpdateAndCheckTraceLimitCB _updateAndCheckTraceLimitCB,
      std::string _taskRegExp);
};
} // namespace bytedance::bolt::exec::trace
