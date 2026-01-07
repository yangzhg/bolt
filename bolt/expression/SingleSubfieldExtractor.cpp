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

#include "bolt/expression/SingleSubfieldExtractor.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/core/Expressions.h"
#include "bolt/core/ITypedExpr.h"
namespace bytedance::bolt::exec {

// Extracts typed expression and its depth from the expression tree
std::pair<const core::ITypedExpr*, size_t>
SingleSubfieldExtractor::fieldTypedExpr(const core::ITypedExpr* root) {
  multipleChainsFound_ = false;

  auto resultOpt = parseSingleChain(root);
  if (!resultOpt || multipleChainsFound_ || resultOpt->first.empty()) {
    return {nullptr, 0};
  }

  auto& [chain, totalDepth] = *resultOpt;
  size_t depth = 0;
  if (totalDepth >= chain.size()) {
    depth = totalDepth - chain.size() + 1;
  }

  return {chain.back(), depth};
}

// Extracts chain of field names and depth from expression tree
std::optional<std::pair<std::vector<std::string>, size_t>>
SingleSubfieldExtractor::extract(const core::ITypedExpr* root) {
  multipleChainsFound_ = false;

  auto resultOpt = parseSingleChain(root);
  if (!resultOpt || multipleChainsFound_) {
    return std::nullopt;
  }

  auto& [chain, totalDepth] = *resultOpt;
  std::vector<std::string> names;
  names.reserve(chain.size());

  for (const auto* node : chain) {
    switch (node->typedExprKind()) {
      case core::kDereference:
        names.push_back(
            static_cast<const core::DereferenceTypedExpr*>(node)->name());
        break;
      case core::kFieldAccess:
        names.push_back(
            static_cast<const core::FieldAccessTypedExpr*>(node)->name());
        break;
      default:
        BOLT_UNREACHABLE();
    }
  }

  size_t depth = 0;
  if (totalDepth >= names.size()) {
    depth = totalDepth - names.size() + 1;
  }

  VLOG(3) << "Extracted " << names.size() << " names with depth " << depth;
  return std::make_pair(names, depth);
}

// Main recursive function to parse expression chains
std::optional<std::pair<std::vector<const core::ITypedExpr*>, size_t>>
SingleSubfieldExtractor::parseSingleChain(const core::ITypedExpr* expr) {
  if (!expr) {
    return std::nullopt;
  }

  VLOG(3) << "Parsing expression of kind: " << expr->typedExprKind();
  switch (expr->typedExprKind()) {
    case core::TypedExprKind::kFieldAccess:
      return parseFieldAccess(expr);

    case core::TypedExprKind::kDereference:
      return parseDereference(expr);

    case core::TypedExprKind::kCast:
      return parseCast(expr);

    case core::TypedExprKind::kCall:
      return parseCall(expr);

    default:
      return std::nullopt;
  }
}

std::optional<std::pair<std::vector<const core::ITypedExpr*>, size_t>>
SingleSubfieldExtractor::parseFieldAccess(const core::ITypedExpr* expr) {
  if (UNLIKELY(
          expr->typedExprKind() != core::TypedExprKind::kFieldAccess ||
          expr == nullptr)) {
    return std::nullopt;
  }

  if (expr->inputs().empty()) {
    return std::make_pair(std::vector<const core::ITypedExpr*>{expr}, 0);
  } else {
    auto childResult = parseSingleChain(expr->inputs()[0].get());
    if (!childResult) {
      return std::make_pair(std::vector<const core::ITypedExpr*>{expr}, 0);
    }

    auto& [childChain, childDepth] = *childResult;
    childChain.push_back(expr);
    return std::make_pair(std::move(childChain), childDepth + 1);
  }
}

std::optional<std::pair<std::vector<const core::ITypedExpr*>, size_t>>
SingleSubfieldExtractor::parseDereference(const core::ITypedExpr* expr) {
  if (UNLIKELY(
          expr == nullptr ||
          expr->typedExprKind() != core::TypedExprKind::kDereference)) {
    return std::nullopt;
  }

  auto deref = static_cast<const core::DereferenceTypedExpr*>(expr);
  if (UNLIKELY(deref->inputs().empty() || deref->name().empty())) {
    return std::nullopt;
  }

  auto childResult = parseSingleChain(deref->inputs()[0].get());
  if (!childResult) {
    return std::nullopt;
  }

  auto& [childChain, childDepth] = *childResult;
  childChain.push_back(deref);
  return std::make_pair(std::move(childChain), childDepth + 1);
}

std::optional<std::pair<std::vector<const core::ITypedExpr*>, size_t>>
SingleSubfieldExtractor::parseCast(const core::ITypedExpr* expr) {
  if (UNLIKELY(
          expr == nullptr ||
          expr->typedExprKind() != core::TypedExprKind::kCast)) {
    return std::nullopt;
  }
  auto castExpr = static_cast<const core::CastTypedExpr*>(expr);
  if (UNLIKELY(castExpr->inputs().empty())) {
    return std::nullopt;
  }

  auto childResult = parseSingleChain(castExpr->inputs()[0].get());
  if (!childResult) {
    return std::nullopt;
  }

  auto& [childChain, childDepth] = *childResult;
  return std::make_pair(std::move(childChain), childDepth + 1);
}

std::optional<std::pair<std::vector<const core::ITypedExpr*>, size_t>>
SingleSubfieldExtractor::parseCall(const core::ITypedExpr* expr) {
  if (UNLIKELY(
          expr == nullptr ||
          expr->typedExprKind() != core::TypedExprKind::kCall)) {
    return std::nullopt;
  }

  auto callExpr = static_cast<const core::CallTypedExpr*>(expr);
  VLOG(3) << "Parsing call expression with " << callExpr->inputs().size()
          << " inputs";

  std::optional<std::pair<std::vector<const core::ITypedExpr*>, size_t>>
      chainSoFar;
  size_t maxChildDepth = 0;

  for (auto& input : callExpr->inputs()) {
    auto childResult = parseSingleChain(input.get());
    if (!childResult) {
      continue;
    }

    auto& [childChain, childDepth] = *childResult;
    if (!childChain.empty()) {
      if (chainSoFar) {
        VLOG(2) << "Multiple chains found in call expression";
        multipleChainsFound_ = true;
        return std::nullopt;
      }
      chainSoFar = std::move(childResult);
      maxChildDepth = childDepth;
    }
  }

  if (!chainSoFar) {
    return std::nullopt;
  }

  return std::make_pair(std::move(chainSoFar->first), maxChildDepth + 1);
}
} // namespace bytedance::bolt::exec