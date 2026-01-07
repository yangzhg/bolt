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

#include "bolt/functions/sparksql/specialforms/SparkCastHooks.h"
#include "bolt/functions/lib/string/StringImpl.h"
#include "bolt/type/TimestampConversion.h"
namespace bytedance::bolt::functions::sparksql {

Timestamp SparkCastHooks::castStringToTimestamp(const StringView& view) const {
  auto resultOpt =
      util::fromTimestampWithTimezoneString(view.data(), view.size());
  if (!resultOpt.has_value()) {
    BOLT_USER_FAIL(
        "Unable to parse timestamp value: \"{}\". "
        "Valid timestamp string pattern is (YYYY-MM-DD HH:MM:SS[.SSSSSS] "
        "[+|-]HH:MM), and can be prefixed with [+-].",
        view.str());
  }

  auto result = resultOpt.value();
  // If the parsed string has timezone information, convert the timestamp at
  // GMT at that time. For example, "1970-01-01 00:00:00 -00:01" is 60 seconds
  // at GMT.
  if (result.second != -1) {
    result.first.toGMT(result.second);
  }

  // If no timezone information is available in the input string, check if we
  // should understand it as being at the session timezone, and if so, convert
  // to GMT.
  else if (sessionTimezone_ != nullptr) {
    result.first.toGMT(*sessionTimezone_);
  }

  return result.first;
}

std::optional<int32_t> SparkCastHooks::castStringToDate(
    const StringView& dateString) const {
  // Allows all patterns supported by Spark:
  // `[+-]yyyy*`
  // `[+-]yyyy*-[m]m`
  // `[+-]yyyy*-[m]m-[d]d`
  // `[+-]yyyy*-[m]m-[d]d *`
  // `[+-]yyyy*-[m]m-[d]dT*`
  // The asterisk `*` in `yyyy*` stands for any numbers.
  // For the last two patterns, the trailing `*` can represent none or any
  // sequence of characters, e.g:
  //   "1970-01-01 123"
  //   "1970-01-01 (BC)"
  return util::castFromDateString(
      removeWhiteSpaces(dateString), false /*isIso8601*/);
}

void SparkCastHooks::castTimestampToString(
    const Timestamp& timestamp,
    exec::StringWriter<false>& out) const {
  static constexpr TimestampToStringOptions options = {
      .precision = TimestampToStringOptions::Precision::kMicroseconds,
      .leadingPositiveSign = true,
      .skipTrailingZeros = true,
      .zeroPaddingYear = true,
      .dateTimeSeparator = ' ',
  };
  out.copy_from(timestamp.toString(options));
  out.finalize();
}

bool SparkCastHooks::legacy() const {
  return false;
}

bool SparkCastHooks::legacyComplexTypeToString() const {
  return legacyCastComplexTypeToString_;
}

StringView SparkCastHooks::removeWhiteSpaces(const StringView& view) const {
  StringView output;
  stringImpl::trimUnicodeWhiteSpace<true, true, StringView, StringView>(
      output, view);
  return output;
}

bool SparkCastHooks::truncate() const {
  return true;
}
} // namespace bytedance::bolt::functions::sparksql
