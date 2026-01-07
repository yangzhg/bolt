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

#include <functional>
#include <optional>
#include <string>
#include "bolt/common/base/Exceptions.h"
#include "folly/Likely.h"
namespace bytedance::bolt {

#define CHECK_ADD(result, adder)                                         \
  if constexpr (                                                         \
      std::is_same<std::decay_t<decltype(result)>, double>::value ||     \
      std::is_same<std::decay_t<decltype(result)>, float>::value ||      \
      std::is_same<                                                      \
          std::decay_t<decltype(result)>,                                \
          bytedance::bolt::LongDecimalType>::value ||                    \
      std::is_same<                                                      \
          std::decay_t<decltype(result)>,                                \
          bytedance::bolt::ShortDecimalType>::value) {                   \
    result = result + adder;                                             \
  } else {                                                               \
    if (UNLIKELY(__builtin_add_overflow(result, adder, &result))) {      \
      BOLT_ARITHMETIC_ERROR("integer overflow: {} + {}", result, adder); \
    }                                                                    \
  }

#define CHECK_SUB(result, sub)                                         \
  if constexpr (                                                       \
      std::is_same<std::decay_t<decltype(result)>, double>::value ||   \
      std::is_same<std::decay_t<decltype(result)>, float>::value ||    \
      std::is_same<                                                    \
          std::decay_t<decltype(result)>,                              \
          bytedance::bolt::LongDecimalType>::value ||                  \
      std::is_same<                                                    \
          std::decay_t<decltype(result)>,                              \
          bytedance::bolt::ShortDecimalType>::value) {                 \
    result = result - sub;                                             \
  } else {                                                             \
    if (UNLIKELY(__builtin_sub_overflow(result, sub, &result))) {      \
      BOLT_ARITHMETIC_ERROR("integer overflow: {} - {}", result, sub); \
    }                                                                  \
  }

#define CHECK_MUL(result, mul)                                         \
  if constexpr (                                                       \
      std::is_same<std::decay_t<decltype(result)>, double>::value ||   \
      std::is_same<std::decay_t<decltype(result)>, float>::value ||    \
      std::is_same<                                                    \
          std::decay_t<decltype(result)>,                              \
          bytedance::bolt::LongDecimalType>::value ||                  \
      std::is_same<                                                    \
          std::decay_t<decltype(result)>,                              \
          bytedance::bolt::ShortDecimalType>::value) {                 \
    result = result * mul;                                             \
  } else {                                                             \
    if (UNLIKELY(__builtin_mul_overflow(result, mul, &result))) {      \
      BOLT_ARITHMETIC_ERROR("integer overflow: {} * {}", result, mul); \
    }                                                                  \
  }

#define CHECK_DIV(result, div)                 \
  if (div == 0) {                              \
    BOLT_ARITHMETIC_ERROR("division by zero"); \
  }                                            \
  result /= div;

#define CHECK_MOD(result, mod)                   \
  if (mod == 0) {                                \
    BOLT_ARITHMETIC_ERROR("cannot mod by zero"); \
  }                                              \
  result %= mod;

#define CHECK_NEG(result)                                                \
  if (UNLIKELY(                                                          \
          result ==                                                      \
          std::numeric_limits<std::decay_t<decltype(result)>>::min())) { \
    BOLT_ARITHMETIC_ERROR("Cannot negate minimum value");                \
  }                                                                      \
  result =                                                               \
      std::negate<std::remove_cv_t<std::decay_t<decltype(result)>>>()(result);

template <typename T>
T checkedPlus(const T& a, const T& b, const char* typeName = "integer") {
  T result;
  if (UNLIKELY(__builtin_add_overflow(a, b, &result))) {
    BOLT_ARITHMETIC_ERROR("{} overflow: {} + {}", typeName, a, b);
  } else {
    return a + b;
  }
}

template <typename T>
T checkedMinus(const T& a, const T& b, const char* typeName = "integer") {
  T result;
  bool overflow = __builtin_sub_overflow(a, b, &result);
  if (UNLIKELY(overflow)) {
    BOLT_ARITHMETIC_ERROR("{} overflow: {} - {}", typeName, a, b);
  }
  return result;
}

template <typename T>
T checkedMultiply(const T& a, const T& b, const char* typeName = "integer") {
  T result;
  bool overflow = __builtin_mul_overflow(a, b, &result);
  if (UNLIKELY(overflow)) {
    BOLT_ARITHMETIC_ERROR("{} overflow: {} * {}", typeName, a, b);
  }
  return result;
}

template <typename T>
T checkedDivide(const T& a, const T& b) {
  if (b == 0) {
    BOLT_ARITHMETIC_ERROR("division by zero");
  }

  // Type T can not represent abs(std::numeric_limits<T>::min()).
  if constexpr (std::is_integral_v<T>) {
    if (UNLIKELY(a == std::numeric_limits<T>::min() && b == -1)) {
      BOLT_ARITHMETIC_ERROR("integer overflow: {} / {}", a, b);
    }
  }
  return a / b;
}

template <typename T>
std::optional<T> checkedModulus(const T& a, const T& b) {
  if (UNLIKELY(b == 0)) {
    // BOLT_ARITHMETIC_ERROR("Cannot divide by 0");
    return std::nullopt;
  }
  // std::numeric_limits<int64_t>::min() % -1 could crash the program since
  // abs(std::numeric_limits<int64_t>::min()) can not be represented in
  // int64_t.
  if (b == -1) {
    return 0;
  }
  return (a % b);
}

template <typename T>
T checkedNegate(const T& a) {
  if (UNLIKELY(a == std::numeric_limits<T>::min())) {
    BOLT_ARITHMETIC_ERROR("Cannot negate minimum value");
  }
  return std::negate<std::remove_cv_t<T>>()(a);
}

} // namespace bytedance::bolt
