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

#include "bolt/common/memory/HashStringAllocator.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::functions::aggregate {

/// An accumulator for a single complex value (a map, an array
/// or a struct).
struct SingleValueAccumulator {
  static constexpr bool supportSerde = false;

  uint32_t getSerializeSize() const {
    BOLT_NYI("getSerializeSize not supported");
  }

  char* serialize(char* dst) {
    BOLT_NYI("serialize not supported");
  }

  char* deserialize(char* src) {
    BOLT_NYI("deserialize not supported");
  }

  void write(
      const BaseVector* vector,
      vector_size_t index,
      HashStringAllocator* allocator);

  void read(
      const VectorPtr& vector,
      vector_size_t index,
      bool exactSize = false) const;

  bool hasValue() const {
    return start_.header != nullptr;
  }

  /// Returns 0 if stored and new values are equal; <0 if stored value is less
  /// then new value; >0 if stored value is greater than new value. If
  /// flags.nullHandlingMode is StopAtNull, returns std::nullopt
  /// in case of null array elements, map values, and struct fields.
  /// If flags.nullHandlingMode is NullAsValue then NULL is considered equal to
  /// NULL.
  std::optional<int32_t> compare(
      const DecodedVector& decoded,
      vector_size_t index,
      CompareFlags compareFlags) const;

  /// Returns memory back to HashStringAllocator.
  void destroy(HashStringAllocator* allocator);

 private:
  HashStringAllocator::Position start_;
};

// An accumulator for a single varchar value
struct SingleVarcharAccumulator {
  static constexpr bool supportSerde = true;

  SingleVarcharAccumulator() {
    setInvalid();
  }
  void write(
      const BaseVector* vector,
      vector_size_t index,
      HashStringAllocator* allocator) {
    StringView newStr =
        vector->asUnchecked<bytedance::bolt::SimpleVector<StringView>>()
            ->valueAt(index);
    if (newStr.isInline()) {
      if (!isInvalid() && !data_.str.isInline()) {
        free(allocator, data_.str.data(), data_.str.size());
      }
      data_.str = newStr;
    } else {
      char* newPtr = nullptr;
      if (isInvalid() || data_.str.isInline()) {
        newPtr = allocate(allocator, newStr.size());
      } else {
        if (data_.str.size() < newStr.size() ||
            data_.str.size() > newStr.size() + MAX_EXTRA_SIZE ||
            // if string is allocated from pool, should not reuse it.
            isFromPool(data_.str.size()) || isFromPool(newStr.size())) {
          free(allocator, data_.str.data(), data_.str.size());

          newPtr = allocate(allocator, newStr.size());
        } else {
          newPtr = const_cast<char*>(data_.str.data());
        }
      }
      memcpy(newPtr, newStr.data(), newStr.size());
      data_.str = StringView(newPtr, newStr.size());
    }
  }

  void read(
      const VectorPtr& vector,
      vector_size_t index,
      bool exactSize = false) const {
    BOLT_DCHECK(!isInvalid());
    vector->asUnchecked<bytedance::bolt::FlatVector<StringView>>()->set(
        index, data_.str);
  }

  bool hasValue() const {
    return !isInvalid();
  }

  int32_t compare(const DecodedVector& decoded, vector_size_t index) const {
    return data_.str.compare(
        decoded.base()
            ->asUnchecked<bytedance::bolt::SimpleVector<StringView>>()
            ->valueAt(decoded.index(index)));
  }

  /// Returns memory back to HashStringAllocator.
  void destroy(HashStringAllocator* allocator) {
    if (!isInvalid() && !data_.str.isInline()) {
      free(allocator, data_.str.data(), data_.str.size());
    }
    setInvalid();
  }

  uint32_t getSerializeSize() const {
    if (hasValue() && !data_.str.isInline()) {
      return data_.str.size();
    } else {
      return 0;
    }
  }

  char* serialize(char* dst) {
    if (hasValue() && !data_.str.isInline()) {
      simd::memcpy(dst, data_.str.data(), data_.str.size());
      return dst + data_.str.size();
    }
    return dst;
  }

  char* deserialize(char* src) {
    if (hasValue() && !data_.str.isInline()) {
      data_.str = StringView(src, data_.str.size());
      return src + data_.str.size();
    }
    return src;
  }

 private:
  static constexpr uint64_t INVALID_VALUE = ~uint64_t{};
  // if used size in string buffer is large than MAX_EXTRA_SIZE, free and
  // allocate a new buffer
  static constexpr size_t MAX_EXTRA_SIZE = 1024;

  static constexpr bool isFromPool(size_t size) {
    return size >= HashStringAllocator::kMaxAlloc;
  }

  char* allocate(HashStringAllocator* allocator, size_t size) {
    if (!isFromPool(size)) {
      auto header = allocator->allocate(size);
      return header->begin();
    } else {
      // For very large strings allocate directly from the pool.
      return (char*)allocator->allocateFromPool(size);
    }
  }

  void free(HashStringAllocator* allocator, const char* p, size_t size) {
    if (!isFromPool(size)) {
      allocator->free(HashStringAllocator::headerOf(p));
    } else {
      allocator->freeToPool((void*)p, size);
    }
  }

  void setInvalid() {
    data_.value[0] = INVALID_VALUE;
    data_.value[1] = INVALID_VALUE;
  }

  bool isInvalid() const {
    return data_.value[0] == INVALID_VALUE && data_.value[1] == INVALID_VALUE;
  }

  union U {
    StringView str;
    uint64_t value[2];
    U() {
      value[0] = INVALID_VALUE;
      value[1] = INVALID_VALUE;
    }
  } data_;
};

} // namespace bytedance::bolt::functions::aggregate
