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

#include "bolt/core/PlanNode.h"

namespace bytedance::bolt::torch {

class TorchNode : public ::bytedance::bolt::core::PlanNode {
 public:
  TorchNode(
      const ::bytedance::bolt::core::PlanNodeId& id,
      bytedance::bolt::core::PlanNodePtr source,
      std::string script,
      ::bytedance::bolt::RowTypePtr outputType);

  const ::bytedance::bolt::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<::bytedance::bolt::core::PlanNodePtr>& sources()
      const override {
    return sources_;
  }

  void addDetails(std::stringstream& stream) const override {
    stream << "torch_script: " << std::endl << script_;
  }

  std::string_view name() const override {
    return operatorName_;
  }

  const std::string& moduleScript() const {
    return script_;
  }

  folly::dynamic serialize() const override;

  // Deserialize TorchNode from (obj, context).
  static ::bytedance::bolt::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

 private:
  // The key of the pytorch script in the serialized storage.
  static inline constexpr std::string_view kScriptKey = "script";
  // The key of the pytorch function name / module id in the serialized storage.
  static inline constexpr std::string_view kFunctionNameKey = "function_name";
  // The key of plan node id in the serialized storage.
  static inline constexpr std::string_view kNodeIdKey = "id";
  // The key of plan node sources in the serialized storage.
  static inline constexpr std::string_view kNodeSourcesKey = "sources";
  // The key of plan node output type in the serialized storage.
  static inline constexpr std::string_view kNodeOutputType = "output_type";

  // Plan node source nodes
  std::vector<::bytedance::bolt::core::PlanNodePtr> sources_;
  // Stored string of this operator name returned by name().
  const std::string operatorName_;
  // The serialized module containing functionName_ to evaluate.
  const std::string script_;
  // The script output type.
  ::bytedance::bolt::RowTypePtr outputType_;
};
} // namespace bytedance::bolt::torch
