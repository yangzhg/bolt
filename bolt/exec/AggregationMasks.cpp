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

#include "bolt/exec/AggregationMasks.h"
#include "bolt/vector/SelectivityVector.h"
namespace bytedance::bolt::exec {

AggregationMasks::AggregationMasks(
    std::vector<std::optional<column_index_t>> maskChannels)
    : maskChannels_{std::move(maskChannels)} {
  for (const auto& maskChannel : maskChannels_) {
    if (maskChannel.has_value()) {
      maskedRows_.insert({maskChannel.value(), SelectivityVector::empty()});
    }
  }
}

void AggregationMasks::addInput(
    const RowVectorPtr& input,
    const SelectivityVector& rows) {
  for (auto& entry : maskedRows_) {
    SelectivityVector& maskedRows = entry.second;
    maskedRows = rows;

    // Get the projection column vector that would be our mask.
    const auto& maskVector = input->childAt(entry.first);

    // Get decoded vector and update the masked selectivity vector.
    decodedMask_.decode(*maskVector, rows);
    if (decodedMask_.isConstantMapping()) {
      if (decodedMask_.isNullAt(rows.begin()) ||
          !decodedMask_.valueAt<bool>(rows.begin())) {
        maskedRows.setValidRange(rows.begin(), rows.end(), false);
        maskedRows.updateBounds();
      }
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        if (decodedMask_.isNullAt(i) || !decodedMask_.valueAt<bool>(i)) {
          maskedRows.setValid(i, false);
        }
      });
      maskedRows.updateBounds();
    }
  }
}

const SelectivityVector* FOLLY_NULLABLE
AggregationMasks::activeRows(int32_t aggregationIndex) const {
  if (maskChannels_[aggregationIndex].has_value()) {
    auto it = maskedRows_.find(maskChannels_[aggregationIndex].value());
    BOLT_CHECK(it != maskedRows_.end());
    return &it->second;
  }

  return nullptr;
}
} // namespace bytedance::bolt::exec
