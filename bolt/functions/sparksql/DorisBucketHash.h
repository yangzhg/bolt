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

#include <folly/hash/Checksum.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "bolt/functions/Macros.h"
#include "bolt/functions/lib/string/StringCore.h"

namespace bytedance::bolt::functions::sparksql {
namespace detail {

constexpr int64_t kDorisBucketHashNull = 558161692LL;

template <typename T>
using UIntFor = std::conditional_t<sizeof(T) == 4, uint32_t, uint64_t>;

template <typename T>
uint64_t crc32LittleEndianInteger(T value) {
  using Unsigned = std::make_unsigned_t<T>;
  Unsigned bits = static_cast<Unsigned>(value);
  std::array<unsigned char, sizeof(T)> bytes{};
  for (size_t i = 0; i < sizeof(T); ++i) {
    bytes[i] = static_cast<unsigned char>((bits >> (8 * i)) & 0xFF);
  }
  return folly::crc32_type(bytes.data(), bytes.size());
}

template <typename T>
uint64_t crc32LittleEndianFloatingPoint(T value) {
  UIntFor<T> bits;
  std::memcpy(&bits, &value, sizeof(T));
  return crc32LittleEndianInteger(bits);
}

inline size_t javaUtf16Length(const StringView& input) {
  size_t length = 0;
  const char* current = input.data();
  const char* end = input.data() + input.size();

  while (current < end) {
    int charSize = 0;
    auto codePoint = stringCore::utf8proc_codepoint(current, end, &charSize);
    if (codePoint < 0 || charSize <= 0) {
      return input.size();
    }
    length += codePoint > 0xFFFF ? 2 : 1;
    current += charSize;
  }

  return length;
}

inline uint64_t crc32JavaString(const StringView& input) {
  const auto length = std::min(input.size(), javaUtf16Length(input));
  return folly::crc32_type(
      reinterpret_cast<const unsigned char*>(input.data()), length);
}

template <typename TInput>
void dorisBucketHashInt32(int64_t& result, const TInput* input) {
  if (input == nullptr) {
    result = kDorisBucketHashNull;
    return;
  }
  result = crc32LittleEndianInteger(static_cast<int32_t>(*input));
}

template <typename TInput>
void dorisBucketHashInt64(int64_t& result, const TInput* input) {
  if (input == nullptr) {
    result = kDorisBucketHashNull;
    return;
  }
  result = crc32LittleEndianInteger(static_cast<int64_t>(*input));
}

template <typename TInput>
void dorisBucketHashFloat(int64_t& result, const TInput* input) {
  if (input == nullptr) {
    result = kDorisBucketHashNull;
    return;
  }
  result = crc32LittleEndianFloatingPoint(static_cast<float>(*input));
}

template <typename TInput>
void dorisBucketHashDouble(int64_t& result, const TInput* input) {
  if (input == nullptr) {
    result = kDorisBucketHashNull;
    return;
  }
  result = crc32LittleEndianFloatingPoint(static_cast<double>(*input));
}

inline void dorisBucketHashString(int64_t& result, const StringView* input) {
  if (input == nullptr) {
    result = kDorisBucketHashNull;
    return;
  }
  result = crc32JavaString(*input);
}

} // namespace detail

template <typename T>
struct DorisBucketHashInt32Function {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void callNullable(int64_t& result, const TInput* input) {
    detail::dorisBucketHashInt32(result, input);
  }
};

template <typename T>
struct DorisBucketHashInt64Function {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void callNullable(int64_t& result, const TInput* input) {
    detail::dorisBucketHashInt64(result, input);
  }
};

template <typename T>
struct DorisBucketHashFloatFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void callNullable(int64_t& result, const TInput* input) {
    detail::dorisBucketHashFloat(result, input);
  }
};

template <typename T>
struct DorisBucketHashDoubleFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void callNullable(int64_t& result, const TInput* input) {
    detail::dorisBucketHashDouble(result, input);
  }
};

template <typename T>
struct DorisBucketHashStringFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void callNullable(
      int64_t& result,
      const arg_type<Varchar>* input) {
    detail::dorisBucketHashString(result, input);
  }
};

} // namespace bytedance::bolt::functions::sparksql
