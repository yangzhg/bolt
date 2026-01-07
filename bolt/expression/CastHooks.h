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

#include "bolt/expression/StringWriter.h"
#include "bolt/type/Timestamp.h"
namespace bytedance::bolt::exec {

/// This class provides cast hooks to allow different behaviors of CastExpr and
/// SparkCastExpr. The main purpose is crate customized cast implementation by
/// taking full usage of existing cast expression.
class CastHooks {
 public:
  virtual ~CastHooks() = default;

  virtual Timestamp castStringToTimestamp(const StringView& view) const = 0;

  virtual std::optional<int32_t> castStringToDate(
      const StringView& dateString) const = 0;

  // Cast from timestamp to string and write the result to string writer.
  virtual void castTimestampToString(
      const Timestamp& timestamp,
      StringWriter<false>& out) const = 0;

  // Returns whether legacy cast semantics are enabled.
  virtual bool legacy() const = 0;

  // Trims all leading and trailing UTF8 whitespaces.
  virtual StringView removeWhiteSpaces(const StringView& view) const = 0;

  // Returns whether to cast to int by truncate.
  virtual bool truncate() const = 0;

  // Returns whether spark legacy cast semantics from complex type to string are
  // enabled.
  virtual bool legacyComplexTypeToString() const = 0;
};
} // namespace bytedance::bolt::exec
