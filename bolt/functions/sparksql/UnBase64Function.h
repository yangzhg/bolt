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

#include "bolt/common/encode/Base64.h"
#include "bolt/functions/Macros.h"

namespace bytedance::bolt::functions::sparksql {

// UnBase64Function decodes a base64-encoded input string into its original
// binary form. It uses the Base64 MIME decoding functions provided by
// bolt::encoding. Returns a Status indicating success or error during
// decoding.
template <typename T>
struct UnBase64Function {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  // Decodes the base64-encoded input and stores the result in 'result'.
  // Returns a Status object indicating success or failure.
  FOLLY_ALWAYS_INLINE void call(
      out_type<Varbinary>& result,
      const arg_type<Varchar>& input) {
    auto decodedSize =
        encoding::Base64::calculateMimeDecodedSize(input.data(), input.size());
    result.resize(decodedSize);
    return encoding::Base64::decodeMime(
        input.data(), input.size(), result.data());
  }
};

} // namespace bytedance::bolt::functions::sparksql
