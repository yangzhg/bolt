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

#include "bolt/expression/PrestoCastHooks.h"
namespace bytedance::bolt::exec {

Timestamp PrestoCastHooks::castStringToTimestamp(const StringView& view) const {
  return util::fromTimestampString(view.data(), view.size(), nullptr);
}

std::optional<int32_t> PrestoCastHooks::castStringToDate(
    const StringView& dateString) const {
  // Cast from string to date allows only ISO 8601 formatted strings:
  // [+-](YYYY-MM-DD).
  auto result = util::castFromDateString(dateString, true /*isIso8601*/);
  if (result.has_value()) {
    return result.value();
  } else {
    BOLT_USER_FAIL(
        "Unable to parse date value: \"{}\"."
        "Valid date string pattern is (YYYY-MM-DD), "
        "and can be prefixed with [+-]",
        dateString.str());
  }
}

void PrestoCastHooks::castTimestampToString(
    const Timestamp& timestamp,
    StringWriter<false>& out) const {
  out.copy_from(
      legacyCast_
          ? util::Converter<TypeKind::VARCHAR, void, util::LegacyCastPolicy>::
                cast(timestamp, nullptr)
          : util::Converter<TypeKind::VARCHAR, void, util::DefaultCastPolicy>::
                cast(timestamp, nullptr));
  out.finalize();
}

bool PrestoCastHooks::legacy() const {
  return legacyCast_;
}

bool PrestoCastHooks::legacyComplexTypeToString() const {
  return false;
}

StringView PrestoCastHooks::removeWhiteSpaces(const StringView& view) const {
  return view;
}

bool PrestoCastHooks::truncate() const {
  return truncateCast_;
}
} // namespace bytedance::bolt::exec
