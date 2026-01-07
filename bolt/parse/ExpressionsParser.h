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

#include <set>
#include "bolt/core/PlanNode.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/parse/IExpr.h"
namespace bytedance::bolt::parse {

/// Hold parsing options.
struct ParseOptions {
  // Retain legacy behavior by default.
  bool parseDecimalAsDouble = true;
  bool parseIntegerAsBigint = true;

  /// SQL functions could be registered with different prefixes by the user.
  /// This parameter is the registered prefix of presto or spark functions,
  /// which helps generate the correct Bolt expression.
  std::string functionPrefix = "";
};

std::shared_ptr<const core::IExpr> parseExpr(
    const std::string& expr,
    const ParseOptions& options);

std::pair<std::shared_ptr<const core::IExpr>, core::SortOrder> parseOrderByExpr(
    const std::string& expr);

std::vector<std::shared_ptr<const core::IExpr>> parseMultipleExpressions(
    const std::string& expr,
    const ParseOptions& options);

} // namespace bytedance::bolt::parse
