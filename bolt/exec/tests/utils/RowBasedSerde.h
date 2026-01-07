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

#include <cstdint>
#include <string_view>
#include "bolt/common/memory/ByteStream.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/DecodedVector.h"
namespace bytedance::bolt::exec {

class RowBasedSerializeStream {
 public:
  RowBasedSerializeStream(uint8_t* data, int32_t size)
      : data_(data), size_(size), position_(0) {}

  template <typename T>
  void appendOne(T val) {
    BOLT_CHECK(size_ > position_ + sizeof(T));
    *reinterpret_cast<T*>(data_ + position_) = val;
    position_ += sizeof(T);
  }

  void appendStringView(StringView sv) {
    BOLT_CHECK(size_ > position_ + sv.size());
    std::memcpy(data_ + position_, sv.data(), sv.size());
    position_ += sv.size();
  }

  template <typename T>
  void append(std::vector<T>& val) {
    for (const auto& v : val) {
      appendOne(v);
    }
  }

  int32_t position() {
    return position_;
  }

  uint8_t* data() {
    return data_;
  }

 private:
  uint8_t* data_;
  int32_t size_;
  int32_t position_;
};

/// Row-wise serialization for use in hash tables and order by.
class RowBasedSerde {
 public:
  /// Serializes value from source[index] into 'out'. The value must not be
  /// null.
  static void serialize(
      const BaseVector& source,
      vector_size_t index,
      RowBasedSerializeStream& out);

  static void deserialize(
      ByteInputStream& in,
      vector_size_t index,
      BaseVector* result,
      bool exactSize = false);

  /// Returns < 0 if 'left' is less than 'right' at 'index', 0 if
  /// equal and > 0 otherwise. flags.nullHandlingMode can be only NullAsValue
  /// and support null-safe equal. Top level rows in right are not allowed to be
  /// null.
  static int32_t compare(
      ByteInputStream& left,
      const DecodedVector& right,
      vector_size_t index,
      CompareFlags flags);

  /// Returns < 0 if 'left' is less than 'right' at 'index', 0 if
  /// equal and > 0 otherwise. flags.nullHandlingMode can be only NullAsValue
  /// and support null-safe equal.
  static int32_t compare(
      ByteInputStream& left,
      ByteInputStream& right,
      const Type* type,
      CompareFlags flags);

  /// Returns < 0 if 'left' is less than 'right' at 'index', 0 if
  /// equal and > 0 otherwise. If flags.nullHandlingMode is StopAtNull,
  /// returns std::nullopt if either 'left' or 'right' value is null or contains
  /// a null. If flags.nullHandlingMode is NullAsValue then NULL is considered
  /// equal to NULL. Top level rows in right are not allowed to be null.
  static std::optional<int32_t> compareWithNulls(
      ByteInputStream& left,
      const DecodedVector& right,
      vector_size_t index,
      CompareFlags flags);

  /// Returns < 0 if 'left' is less than 'right' at 'index', 0 if
  /// equal and > 0 otherwise. If flags.nullHandlingMode is StopAtNull,
  /// returns std::nullopt if either 'left' or 'right' value is null or contains
  /// a null. If flags.nullHandlingMode is NullAsValue then NULL is considered
  /// equal to NULL.
  static std::optional<int32_t> compareWithNulls(
      ByteInputStream& left,
      ByteInputStream& right,
      const Type* type,
      CompareFlags flags);

  static uint64_t hash(ByteInputStream& data, const Type* type);
};

} // namespace bytedance::bolt::exec
