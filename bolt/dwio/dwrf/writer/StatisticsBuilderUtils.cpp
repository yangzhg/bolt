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

#include "bolt/dwio/dwrf/writer/StatisticsBuilderUtils.h"
namespace bytedance::bolt::dwrf {

void StatisticsBuilderUtils::addValues(
    StatisticsBuilder& builder,
    const VectorPtr& vector,
    const common::Ranges& ranges) {
  auto nulls = vector->rawNulls();
  if (vector->mayHaveNulls()) {
    for (auto& pos : ranges) {
      if (bits::isBitNull(nulls, pos)) {
        builder.setHasNull();
      } else {
        builder.increaseValueCount();
      }
    }
  } else {
    builder.increaseValueCount(ranges.size());
  }
}

void StatisticsBuilderUtils::addValues(
    BooleanStatisticsBuilder& builder,
    const VectorPtr& vector,
    const common::Ranges& ranges) {
  auto nulls = vector->rawNulls();
  auto vals = vector->as<FlatVector<bool>>()->asRange();
  if (vector->mayHaveNulls()) {
    for (auto& pos : ranges) {
      if (bits::isBitNull(nulls, pos)) {
        builder.setHasNull();
      } else {
        builder.addValues(vals[pos]);
      }
    }
  } else {
    for (auto& pos : ranges) {
      builder.addValues(vals[pos]);
    }
  }
}

void StatisticsBuilderUtils::addValues(
    BooleanStatisticsBuilder& builder,
    const DecodedVector& vector,
    const common::Ranges& ranges) {
  if (vector.mayHaveNulls()) {
    for (auto& pos : ranges) {
      if (vector.isNullAt(pos)) {
        builder.setHasNull();
      } else {
        builder.addValues(vector.valueAt<bool>(pos));
      }
    }
  } else {
    for (auto& pos : ranges) {
      builder.addValues(vector.valueAt<bool>(pos));
    }
  }
}

void StatisticsBuilderUtils::addValues(
    StringStatisticsBuilder& builder,
    const VectorPtr& vector,
    const common::Ranges& ranges) {
  auto nulls = vector->rawNulls();
  auto data = vector->asFlatVector<StringView>()->rawValues();
  if (vector->mayHaveNulls()) {
    for (auto& pos : ranges) {
      if (bits::isBitNull(nulls, pos)) {
        builder.setHasNull();
      } else {
        builder.addValues(folly::StringPiece{data[pos]});
      }
    }
  } else {
    for (auto& pos : ranges) {
      builder.addValues(folly::StringPiece{data[pos]});
    }
  }
}

void StatisticsBuilderUtils::addValues(
    BinaryStatisticsBuilder& builder,
    const VectorPtr& vector,
    const common::Ranges& ranges) {
  auto nulls = vector->rawNulls();
  auto data = vector->asFlatVector<StringView>()->rawValues();
  if (vector->mayHaveNulls()) {
    for (auto& pos : ranges) {
      if (bits::isBitNull(nulls, pos)) {
        builder.setHasNull();
      } else {
        builder.addValues(data[pos].size());
      }
    }
  } else {
    for (auto& pos : ranges) {
      builder.addValues(data[pos].size());
    }
  }
}

} // namespace bytedance::bolt::dwrf
