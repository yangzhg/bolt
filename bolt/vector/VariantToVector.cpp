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

#include "bolt/vector/VariantToVector.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::core {
namespace {

template <TypeKind KIND>
ArrayVectorPtr variantArrayToVectorImpl(
    const TypePtr& arrayType,
    const std::vector<variant>& variantArray,
    bolt::memory::MemoryPool* pool) {
  using T = typename TypeTraits<KIND>::NativeType;

  // First generate internal arrayVector elements.
  const size_t variantArraySize = variantArray.size();

  // Create array elements flat vector.
  auto arrayElements = BaseVector::create<FlatVector<T>>(
      arrayType->childAt(0), variantArraySize, pool);

  // Populate internal array elements (flat vector).
  for (vector_size_t i = 0; i < variantArraySize; i++) {
    const auto& value = variantArray[i];
    if (!value.isNull()) {
      // `getOwnedValue` copies the content to its internal buffers (in case of
      // string/StringView); no-op for other primitive types.
      arrayElements->set(i, T(value.value<KIND>()));
    } else {
      arrayElements->setNull(i, true);
    }
  }

  // Create ArrayVector around the FlatVector containing array elements.
  BufferPtr offsets = allocateOffsets(1, pool);
  BufferPtr sizes = allocateSizes(1, pool);

  auto rawSizes = sizes->asMutable<vector_size_t>();
  rawSizes[0] = variantArraySize;

  return std::make_shared<ArrayVector>(
      pool, arrayType, nullptr, 1, offsets, sizes, arrayElements);
}
} // namespace

ArrayVectorPtr variantArrayToVector(
    const TypePtr& arrayType,
    const std::vector<variant>& variantArray,
    bolt::memory::MemoryPool* pool) {
  BOLT_CHECK_EQ(TypeKind::ARRAY, arrayType->kind());

  if (arrayType->childAt(0)->isUnKnown()) {
    return variantArrayToVectorImpl<TypeKind::UNKNOWN>(
        arrayType, variantArray, pool);
  }

  return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
      variantArrayToVectorImpl,
      arrayType->childAt(0)->kind(),
      arrayType,
      variantArray,
      pool);
}

} // namespace bytedance::bolt::core
