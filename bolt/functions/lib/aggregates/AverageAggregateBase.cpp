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

#include "bolt/functions/lib/aggregates/AverageAggregateBase.h"
namespace bytedance::bolt::functions::aggregate {

void checkAvgIntermediateType(const TypePtr& type) {
  BOLT_USER_CHECK(
      type->isRow() || type->isVarbinary(),
      "Input type for final average must be row type or varbinary type, find {}",
      type->toString());
  if (type->kind() == TypeKind::VARBINARY) {
    return;
  }
  BOLT_USER_CHECK(
      type->childAt(0)->kind() == TypeKind::DOUBLE ||
          type->childAt(0)->isLongDecimal(),
      "Input type for sum in final average must be double or long decimal type, find {}",
      type->childAt(0)->toString());
  BOLT_USER_CHECK_EQ(
      type->childAt(1)->kind(),
      TypeKind::BIGINT,
      "Input type for count in final average must be bigint type.");
}

} // namespace bytedance::bolt::functions::aggregate
