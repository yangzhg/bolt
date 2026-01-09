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

#include <optional>
#include <string>
#include <vector>
namespace bytedance::bolt::core {
class ITypedExpr;
class FieldAccessTypedExpr;
class DereferenceTypedExpr;
class CastTypedExpr;
class CallTypedExpr;
} // namespace bytedance::bolt::core
namespace bytedance::bolt::exec {
class SingleSubfieldExtractor {
 public:
  // Public entry point. Takes a Bolt ITypedExpr pointer.
  // Returns a vector of field names if exactly one subfield chain,
  // empty if no field references, or std::nullopt if multiple or invalid.
  std::optional<std::pair<std::vector<std::string>, size_t>> extract(
      const core::ITypedExpr* root);
  std::pair<const core::ITypedExpr*, size_t> fieldTypedExpr(
      const core::ITypedExpr* root);

 private:
  std::optional<std::pair<std::vector<const core::ITypedExpr*>, size_t>>
  parseSingleChain(const core::ITypedExpr* expr);

  std::optional<std::pair<std::vector<const core::ITypedExpr*>, size_t>>
  parseFieldAccess(const core::ITypedExpr* expr);
  std::optional<std::pair<std::vector<const core::ITypedExpr*>, size_t>>
  parseDereference(const core::ITypedExpr* expr);
  std::optional<std::pair<std::vector<const core::ITypedExpr*>, size_t>>
  parseCast(const core::ITypedExpr* expr);
  std::optional<std::pair<std::vector<const core::ITypedExpr*>, size_t>>
  parseCall(const core::ITypedExpr* expr);

 private:
  bool multipleChainsFound_{false};
};

} // namespace bytedance::bolt::exec
