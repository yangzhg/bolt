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

#include <memory>
#include <string>
#include <vector>
namespace bytedance::bolt::core {
class IExpr;
class SortOrder;
} // namespace bytedance::bolt::core
namespace bytedance::bolt::duckdb {
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

// Parses an input expression using DuckDB's internal postgresql-based parser,
// converting it to an IExpr tree. Takes a single expression as input.
//
// One caveat to keep in mind when using DuckDB's parser is that all identifiers
// are lower-cased, what prevents you to use functions and column names
// containing upper case letters (e.g: "concatRow" will be parsed as
// "concatrow").
std::shared_ptr<const core::IExpr> parseExpr(
    const std::string& exprString,
    const ParseOptions& options);

std::vector<std::shared_ptr<const core::IExpr>> parseMultipleExpressions(
    const std::string& exprString,
    const ParseOptions& options);

struct AggregateExpr {
  std::shared_ptr<const core::IExpr> expr;
  std::vector<std::pair<std::shared_ptr<const core::IExpr>, core::SortOrder>>
      orderBy;
  bool distinct{false};
  std::shared_ptr<const core::IExpr> maskExpr{nullptr};
};

/// Parses aggregate function call expression with optional ORDER by clause.
/// Examples:
///     sum(a)
///     sum(a) as s
///     array_agg(x ORDER BY y DESC)
AggregateExpr parseAggregateExpr(
    const std::string& exprString,
    const ParseOptions& options);

// Parses an ORDER BY clause using DuckDB's internal postgresql-based parser,
// converting it to a pair of an IExpr tree and a core::SortOrder. Uses ASC
// NULLS LAST as the default sort order.
std::pair<std::shared_ptr<const core::IExpr>, core::SortOrder> parseOrderByExpr(
    const std::string& exprString);

// Parses a WINDOW function SQL string using DuckDB's internal postgresql-based
// parser. Window Functions are executed by Bolt Window PlanNodes and not the
// expression evaluation. So we cannot use an IExpr based API. The structures
// below capture all the metadata needed from the window function SQL string
// for usage in the WindowNode plan node.
enum class WindowType { kRows, kRange };

enum class BoundType {
  kCurrentRow,
  kUnboundedPreceding,
  kUnboundedFollowing,
  kPreceding,
  kFollowing
};

struct IExprWindowFrame {
  WindowType type;
  BoundType startType;
  std::shared_ptr<const core::IExpr> startValue;
  BoundType endType;
  std::shared_ptr<const core::IExpr> endValue;
};

struct IExprWindowFunction {
  std::shared_ptr<const core::IExpr> functionCall;
  IExprWindowFrame frame;
  bool ignoreNulls;

  std::vector<std::shared_ptr<const core::IExpr>> partitionBy;
  std::vector<std::pair<std::shared_ptr<const core::IExpr>, core::SortOrder>>
      orderBy;
};

IExprWindowFunction parseWindowExpr(
    const std::string& windowString,
    const ParseOptions& options);

} // namespace bytedance::bolt::duckdb
