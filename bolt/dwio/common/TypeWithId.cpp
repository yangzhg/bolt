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

#include "bolt/dwio/common/TypeWithId.h"
#include "bolt/dwio/common/exception/Exception.h"
namespace bytedance::bolt::dwio::common {

using bolt::Type;
using bolt::TypeKind;

TypeWithId::TypeWithId(
    std::shared_ptr<const Type> type,
    std::vector<std::shared_ptr<const TypeWithId>>&& children,
    uint32_t id,
    uint32_t maxId,
    uint32_t column)
    : type_{std::move(type)},
      parent_{nullptr},
      id_{id},
      maxId_{maxId},
      column_{column},
      children_{std::move(children)} {
  for (auto& child : children_) {
    const_cast<const TypeWithId*&>(child->parent_) = this;
  }
}

std::shared_ptr<const TypeWithId> TypeWithId::create(
    const std::shared_ptr<const Type>& root,
    uint32_t next) {
  return create(root, next, 0);
}

uint32_t TypeWithId::size() const {
  return children_.size();
}

const std::shared_ptr<const TypeWithId>& TypeWithId::childAt(
    uint32_t idx) const {
  return children_.at(idx);
}

std::shared_ptr<const TypeWithId> TypeWithId::create(
    const std::shared_ptr<const Type>& type,
    uint32_t& next,
    uint32_t column) {
  DWIO_ENSURE_NOT_NULL(type);
  const uint32_t myId = next++;
  std::vector<std::shared_ptr<const TypeWithId>> children{};
  children.reserve(type->size());
  auto offset = 0;
  for (const auto& child : *type) {
    children.emplace_back(create(
        child,
        next,
        (myId == 0 && type->kind() == TypeKind::ROW) ? offset++ : column));
  }
  const uint32_t maxId = next - 1;
  return std::make_shared<const TypeWithId>(
      type, std::move(children), myId, maxId, column);
}

} // namespace bytedance::bolt::dwio::common
