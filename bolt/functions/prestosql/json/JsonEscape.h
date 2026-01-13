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
#include <string>
#include "bolt/common/base/Exceptions.h"
namespace bytedance::bolt::functions {
namespace JsonEscape {

namespace {

class Ptr {
 public:
  Ptr(char* ptr, std::size_t offset)
      : _beg(ptr), _cur(ptr), _end(ptr + offset) {}

  char*& operator++() // prefix
  {
    checkBounds(_cur + 1);
    return ++_cur;
  }

  char* operator++(int) // postfix
  {
    checkBounds(_cur + 1);
    char* tmp = _cur++;
    return tmp;
  }

  char*& operator--() // prefix
  {
    checkBounds(_cur - 1);
    return --_cur;
  }

  char* operator--(int) // postfix
  {
    checkBounds(_cur - 1);
    char* tmp = _cur--;
    return tmp;
  }

  char*& operator+=(int incr) {
    checkBounds(_cur + incr);
    return _cur += incr;
  }

  char*& operator-=(int decr) {
    checkBounds(_cur - decr);
    return _cur -= decr;
  }

  operator char*() const {
    return _cur;
  }

  std::size_t span() const {
    return _end - _beg;
  }

 private:
  void checkBounds(char* ptr) {
    if (UNLIKELY(ptr > _end)) {
      BOLT_FAIL("Pointer out of bound!");
    }
  }

  const char* _beg;
  char* _cur;
  const char* _end;
};

template <typename T>
inline bool uIntToStr(
    T value,
    unsigned short base,
    char* result,
    std::size_t& size,
    bool prefix,
    int width = -1,
    char fill = ' ',
    char thSep = 0) {
  if (base < 2 || base > 0x10) {
    *result = '\0';
    return false;
  }

  Ptr ptr(result, size);
  int thCount = 0;
  T tmpVal;
  do {
    tmpVal = value;
    value /= base;
    *ptr++ = "FEDCBA9876543210123456789ABCDEF"[15 + (tmpVal - value * base)];
    if (thSep && (base == 10) && (++thCount == 3)) {
      *ptr++ = thSep;
      thCount = 0;
    }
  } while (value);

  if ('0' == fill) {
    if (prefix && base == 010)
      --width;
    if (prefix && base == 0x10)
      width -= 2;
    while ((ptr - result) < width)
      *ptr++ = fill;
  }

  if (prefix && base == 010)
    *ptr++ = '0';
  else if (prefix && base == 0x10) {
    *ptr++ = 'x';
    *ptr++ = '0';
  }

  if ('0' != fill) {
    while ((ptr - result) < width)
      *ptr++ = fill;
  }

  size = ptr - result;
  *ptr-- = '\0';

  char* ptrr = result;
  char tmp;
  while (ptrr < ptr) {
    tmp = *ptr;
    *ptr-- = *ptrr;
    *ptrr++ = tmp;
  }

  return true;
}

inline void appendHex(std::string& str, int value, int width) {
  constexpr unsigned NF_MAX_INT_STRING_LEN = 32;
  char result[NF_MAX_INT_STRING_LEN];
  std::size_t sz = NF_MAX_INT_STRING_LEN;
  uIntToStr(
      static_cast<unsigned int>(value), 0x10, result, sz, false, width, '0');
  str.append(result, sz);
}

inline int32_t isOutOfBMP(
    std::string_view::const_iterator start,
    std::string_view::const_iterator end) {
  size_t remainingBytes =
      0; // Number of bytes remaining in a UTF-8 character sequence.
  uint32_t codepoint = 0;

  if ((*start & 0x80) == 0x00) {
    codepoint = *start;
    remainingBytes = 0;
  } else if ((*start & 0xE0) == 0xC0) {
    codepoint = *start & 0x1F;
    remainingBytes = 1;
  } else if ((*start & 0xF0) == 0xE0) {
    codepoint = *start & 0x0F;
    remainingBytes = 2;
  } else if ((*start & 0xF8) == 0xF0) {
    codepoint = *start & 0x07;
    remainingBytes = 3;
  } else {
    // Invalid UTF-8 starting byte
    return -1;
  }

  start++;

  int32_t characterSize = remainingBytes + 1;

  while (remainingBytes > 0) {
    if (start == end || (*start & 0xC0) != 0x80) {
      // Invalid UTF-8 continuation byte
      return -1;
    }
    codepoint = (codepoint << 6) | (*start & 0x3F);
    start++;
    remainingBytes--;
  }

  // Code point validity check
  if (codepoint > 0x10FFFF || (codepoint >= 0xD800 && codepoint <= 0xDFFF)) {
    // Beyond Unicode range or a surrogate code point
    return -1;
  }
  if (codepoint > 0xFFFF) {
    return characterSize;
  } else {
    return -1;
  }
}

} // namespace

inline std::string escape(
    const std::string_view::const_iterator& begin,
    const std::string_view::const_iterator& end,
    bool strictJSON) {
  constexpr uint32_t offsetsFromUTF8[6] = {
      0x00000000UL,
      0x00003080UL,
      0x000E2080UL,
      0x03C82080UL,
      0xFA082080UL,
      0x82082080UL};

  std::string result;

  std::string_view::const_iterator it = begin;

  while (it != end) {
    uint32_t ch = 0;
    unsigned int sz = 0;

    do {
      ch <<= 6;
      ch += (unsigned char)*it++;
      sz++;
    } while (it != end && (*it & 0xC0) == 0x80 && sz < 6);
    ch -= offsetsFromUTF8[sz - 1];

    if (ch == '\n')
      result += "\\n";
    else if (ch == '\t')
      result += "\\t";
    else if (ch == '\r')
      result += "\\r";
    else if (ch == '\b')
      result += "\\b";
    else if (ch == '\f')
      result += "\\f";
    else if (ch == '\v')
      result += (strictJSON ? "\\u000B" : "\\v");
    else if (ch == '\a')
      result += (strictJSON ? "\\u0007" : "\\a");
    else if (ch == '\\')
      result += "\\\\";
    else if (ch == '\"')
      result += "\\\"";
    else if (ch == '/')
      result += "\\/";
    else if (ch == '\0')
      result += "\\u0000";
    else if (ch < 32 || ch == 0x7f) {
      result += "\\u";
      appendHex(result, (unsigned short)ch, 4);
    } else if (ch > 0xFFFF) {
      ch -= 0x10000;
      result += "\\u";
      appendHex(result, (unsigned short)((ch >> 10) & 0x03ff) + 0xd800, 4);
      result += "\\u";
      appendHex(result, (unsigned short)(ch & 0x03ff) + 0xdc00, 4);
    } else if (ch >= 0x80 && ch <= 0xFFFF) {
      result += "\\u";
      appendHex(result, (unsigned short)ch, 4);
    } else {
      result += (char)ch;
    }
  }
  return result;
}

template <bool unescapeAscii = true>
inline void escapeOutputString(std::string_view src, std::string& ans) {
  constexpr double stringExpandFactor = 1.5;
  ans.reserve(ans.size() + src.size() * stringExpandFactor);
  std::string_view::const_iterator it = src.begin(), end = src.end();

  while (it != end) {
    // From https://datatracker.ietf.org/doc/html/rfc8259#section-7
    if ((*it >= 0 && *it <= 31) || (*it == '"') || (*it == '\\')) {
      if constexpr (unescapeAscii) {
        ans += escape(it, it + 1, true);
        it++;
      } else {
        ans.push_back(*it);
        it++;
      }
    } else {
      // emoji process
      int32_t characterSize = isOutOfBMP(it, end);
      if (characterSize > 0) {
        ans += escape(it, it + characterSize, true);
        it += characterSize;
      } else {
        ans.push_back(*it);
        it++;
      }
    }
  }
}

} // namespace JsonEscape
} // namespace bytedance::bolt::functions
