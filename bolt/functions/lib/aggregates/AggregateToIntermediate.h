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
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::functions::aggregate {

template <typename FromType, typename ToType>
inline void assignToIntermediate(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args,
    VectorPtr& result) {
  const auto& input = args[0];
  BOLT_CHECK(result->typeKind() == CppToType<ToType>::typeKind);
  BOLT_CHECK(input->typeKind() == CppToType<FromType>::typeKind);
  if (rows.isAllSelected() && std::is_same_v<FromType, ToType>) {
    result = input;
    return;
  }
  result->resize(input->size());
  DecodedVector decoded(*input, rows);
  auto flatVector = result->asFlatVector<ToType>();
  auto resultValues = flatVector->mutableRawValues();
  auto rawNulls = result->mutableRawNulls();
  // set all rows to null
  bits::setAllNull(rawNulls, input->size());
  rows.applyToSelected([&](vector_size_t i) {
    if (!decoded.isNullAt(i)) {
      resultValues[i] = decoded.valueAt<FromType>(i);
      bits::clearNull(rawNulls, i);
    }
  });
}

inline void copyToIntermediate(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args,
    VectorPtr& result) {
  const auto& input = args[0];
  BOLT_CHECK(input->type()->equivalent(*result->type()));
  if (rows.isAllSelected()) {
    result = input;
    return;
  }
  result->resize(input->size());
  // set all rows to null
  bits::setAllNull(result->mutableRawNulls(), input->size());
  result->copy(input.get(), rows, nullptr, false);
}

template <bool nullAsZero = true>
inline void countToIntermediate(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args,
    VectorPtr& result) {
  const auto& input = args[0];
  BOLT_CHECK(result->typeKind() == TypeKind::BIGINT);
  result->resize(input->size());

  auto resultValues = result->asFlatVector<int64_t>()->mutableRawValues();
  auto rawNulls = result->mutableRawNulls();
  if (nullAsZero) {
    std::fill(resultValues, resultValues + input->size(), 0);
    // clear all nulls
    bits::clearAllNull(rawNulls, input->size());
  } else {
    // set all rows to null
    bits::setAllNull(rawNulls, input->size());
  }
  rows.applyToSelected([&](vector_size_t i) {
    if (!input->isNullAt(i)) {
      resultValues[i] = 1;
      // nullAsZero has already cleared null
      if (!nullAsZero) {
        bits::clearNull(rawNulls, i);
      }
    }
  });
}
} // namespace bytedance::bolt::functions::aggregate