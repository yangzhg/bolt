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

#include "bolt/common/memory/HashStringAllocator.h"
namespace bytedance::bolt::aggregate::prestosql {

/// Stores non-inlined strings in memory blocks managed by
/// HashStringAllocator.
struct Strings {
  /// Header of the first block allocated via HashStringAllocator.
  HashStringAllocator::Header* firstBlock{nullptr};

  /// Header of the last block allocated via HashStringAllocator. Blocks
  /// are forming a singly-linked list via Header::nextContinued.
  /// HashStringAllocator takes care of linking the blocks.
  HashStringAllocator::Position currentBlock{nullptr, nullptr};

  /// Maximum size of a string passed to 'append'. Used to calculate the amount
  /// of space to reserve for future strings.
  size_t maxStringSize = 0;

  /// Copies the string into contiguous memory allocated via
  /// HashStringAllocator. Returns StringView over the copy.
  StringView append(StringView value, HashStringAllocator& allocator);

  /// Frees memory used by the strings. StringViews returned from 'append'
  /// become invalid after this call.
  void free(HashStringAllocator& allocator);
};
} // namespace bytedance::bolt::aggregate::prestosql
