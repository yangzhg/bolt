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

#include "bolt/common/base/CheckedArithmetic.h"

// Forwarding the definitions here so that codegen can still use functions in
// this namespace.
namespace bytedance::bolt::functions {

template <typename T>
FOLLY_ALWAYS_INLINE T checkedPlus(const T& a, const T& b) {
  return bytedance::bolt::checkedPlus(a, b);
}

template <typename T>
T checkedMinus(const T& a, const T& b) {
  return bytedance::bolt::checkedMinus(a, b);
}

template <typename T>
T checkedMultiply(const T& a, const T& b) {
  return bytedance::bolt::checkedMultiply(a, b);
}

template <typename T>
T checkedDivide(const T& a, const T& b) {
  return bytedance::bolt::checkedDivide(a, b);
}

template <typename T>
std::optional<T> checkedModulus(const T& a, const T& b) {
  return bytedance::bolt::checkedModulus(a, b);
}

template <typename T>
T checkedNegate(const T& a) {
  return bytedance::bolt::checkedNegate(a);
}

} // namespace bytedance::bolt::functions
