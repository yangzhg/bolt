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

#include "bolt/connectors/Connector.h"
namespace bytedance::bolt::exec {

struct Split {
  std::shared_ptr<bolt::connector::ConnectorSplit> connectorSplit;
  int32_t groupId{-1}; // Bucketed group id (-1 means 'none').
  uint32_t multiSplitInnerSize{1};
  const std::string multiSplitUuid{"mock"};

  Split() = default;

  explicit Split(
      std::shared_ptr<bolt::connector::ConnectorSplit>&& connectorSplit,
      int32_t groupId = -1,
      int32_t multiSplitInnerSize = 1,
      std::string multiSplitUuid = "mock")
      : connectorSplit(std::move(connectorSplit)),
        groupId(groupId),
        multiSplitInnerSize(multiSplitInnerSize),
        multiSplitUuid(std::move(multiSplitUuid)) {}

  Split(Split&& other)
      : connectorSplit(std::move(other.connectorSplit)),
        groupId(other.groupId),
        multiSplitInnerSize(other.multiSplitInnerSize),
        multiSplitUuid(std::move(other.multiSplitUuid)) {}

  Split(const Split& other)
      : connectorSplit(other.connectorSplit),
        groupId(other.groupId),
        multiSplitInnerSize(other.multiSplitInnerSize),
        multiSplitUuid(other.multiSplitUuid) {}

  void operator=(Split&& other) {
    connectorSplit = std::move(other.connectorSplit);
    groupId = other.groupId;
  }

  void operator=(const Split& other) {
    connectorSplit = other.connectorSplit;
    groupId = other.groupId;
  }

  inline bool hasConnectorSplit() const {
    return connectorSplit != nullptr;
  }

  inline bool hasGroup() const {
    return groupId != -1;
  }

  inline uint32_t getMultiSplitInnerSize() const {
    return multiSplitInnerSize;
  }

  inline const std::string& getMultiSplitUuid() const {
    return multiSplitUuid;
  }

  std::string toString() const {
    return fmt::format(
        "Split: [{}] {}",
        hasConnectorSplit() ? connectorSplit->toString() : "NULL",
        groupId);
  }
};

} // namespace bytedance::bolt::exec
