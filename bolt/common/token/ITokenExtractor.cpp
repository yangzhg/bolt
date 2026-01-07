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

#include "bolt/common/token/ITokenExtractor.h"
#include <bit>
namespace bytedance::bolt {

template <typename T>
inline uint32_t getLeadingZeroBitsUnsafe(T x) {
  assert(x != 0);

  if constexpr (sizeof(T) <= sizeof(unsigned int)) {
    return __builtin_clz(x);
  } else if constexpr (sizeof(T) <= sizeof(unsigned long int)) /// NOLINT
  {
    return __builtin_clzl(x);
  } else
    return __builtin_clzll(x);
}

inline bool isASCII(char c) {
  return static_cast<unsigned char>(c) < 0x80;
}

inline bool isLowerAlphaASCII(char c) {
  return (c >= 'a' && c <= 'z');
}

inline bool isUpperAlphaASCII(char c) {
  return (c >= 'A' && c <= 'Z');
}

inline bool isAlphaASCII(char c) {
  return isLowerAlphaASCII(c) || isUpperAlphaASCII(c);
}

inline bool isNumericASCII(char c) {
  /// This is faster than
  /// return uint8_t(uint8_t(c) - uint8_t('0')) < uint8_t(10);
  /// on Intel CPUs when compiled by gcc 8.
  return (c >= '0' && c <= '9');
}

inline bool isAlphaNumericASCII(char c) {
  return isAlphaASCII(c) || isNumericASCII(c);
}

namespace UTF8 {
/** Returns log2 of number, rounded down.
 * Compiles to single 'bsr' instruction on x86.
 * For zero argument, result is unspecified.
 */
template <typename T>
inline uint32_t bitScanReverse(T x) {
  return (std::max<size_t>(sizeof(T), sizeof(unsigned int))) * 8 - 1 -
      getLeadingZeroBitsUnsafe(x);
}

/// returns UTF-8 code point sequence length judging by it's first octet
inline size_t seqLength(const uint8_t first_octet) {
  if (first_octet < 0x80 || first_octet >= 0xF8) /// The specs of UTF-8.
    return 1;

  const size_t bits = 8;
  const auto first_zero = bitScanReverse(static_cast<uint8_t>(~first_octet));

  return bits - 1 - first_zero;
}
} // namespace UTF8

bool NgramTokenExtractor::nextInStringLike(
    const char* data,
    uint32_t length,
    uint32_t* pos,
    std::string& token) const {
  token.clear();

  uint32_t code_points = 0;
  bool escaped = false;
  for (uint32_t i = *pos; i < length;) {
    if (escaped && (data[i] == '%' || data[i] == '_' || data[i] == '\\')) {
      token += data[i];
      ++code_points;
      escaped = false;
      ++i;
    } else if (
        !escaped && (data[i] == '%' || data[i] == '_' || data[i] == ' ')) {
      /// This token is too small, go to the next.
      token.clear();
      code_points = 0;
      escaped = false;
      *pos = ++i;
    } else if (!escaped && data[i] == '\\') {
      escaped = true;
      ++i;
    } else {
      const uint32_t sz = UTF8::seqLength(static_cast<uint8_t>(data[i]));
      for (uint32_t j = 0; j < sz; ++j)
        token += data[i + j];
      i += sz;
      ++code_points;
      escaped = false;
    }

    if (code_points == ngram_) {
      *pos += UTF8::seqLength(static_cast<uint8_t>(data[*pos]));
      return true;
    }
  }

  return false;
}

bool SplitTokenExtractor::nextInStringLike(
    const char* data,
    uint32_t length,
    uint32_t* pos,
    std::string& token) const {
  token.clear();
  bool bad_token = false; // % or _ before token
  bool escaped = false;
  while (*pos < length) {
    if (!escaped && (data[*pos] == '%' || data[*pos] == '_')) {
      token.clear();
      bad_token = true;
      ++*pos;
    } else if (!escaped && data[*pos] == '\\') {
      escaped = true;
      ++*pos;
    } else if (isASCII(data[*pos]) && !isAlphaNumericASCII(data[*pos])) {
      if (!bad_token && !token.empty())
        return true;

      token.clear();
      bad_token = false;
      escaped = false;
      ++*pos;
    } else {
      const uint32_t sz = UTF8::seqLength(static_cast<uint8_t>(data[*pos]));
      for (uint32_t j = 0; j < sz; ++j) {
        token += data[*pos];
        ++*pos;
      }
      escaped = false;
    }
  }

  return !bad_token && !token.empty();
}

std::vector<std::string> ITokenExtractor::splitToStringVector(
    const char* data,
    uint32_t length) {
  std::vector<std::string> tokens;
  bool escaped = false;
  int32_t pos = 0, start = 0;
  for (; pos < length; ++pos) {
    if (escaped) {
      escaped = false;
    } else if (!escaped && data[pos] == '\\') {
      escaped = true;
    } else if (!escaped && data[pos] == '%') {
      if (pos > start) {
        tokens.emplace_back(data + start, pos - start);
      }
      start = pos + 1;
      escaped = false;
    } else {
      escaped = false;
    }
  }

  if (pos > start && !escaped) {
    tokens.emplace_back(data + start, pos - start);
  }

  std::sort(tokens.begin(), tokens.end(), [](std::string& a, std::string& b) {
    return a.length() > b.length();
  });

  return tokens;
}
} // namespace bytedance::bolt
