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

#include "bolt/buffer/Buffer.h"
namespace bytedance::bolt {

/// Helper struct to lazily initialize nulls buffer on first null.
struct NullsBuilder {
  NullsBuilder(vector_size_t size, memory::MemoryPool* pool)
      : size_{size}, pool_{pool} {}

  /// Marks specified row as null. Allocates and initializes null buffer if this
  /// is the first null.
  void setNull(vector_size_t row) {
    if (nulls_ == nullptr) {
      nulls_ = AlignedBuffer::allocate<bool>(size_, pool_, bits::kNotNull);
      rawNulls_ = nulls_->asMutable<uint64_t>();
    }
    bits::setNull(rawNulls_, row, true);
  }

  /// Returns nulls buffer or nullptr if no nulls were added (e.g. setNull was
  /// never called).
  BufferPtr build() const {
    return nulls_;
  }

 private:
  const vector_size_t size_;
  memory::MemoryPool* pool_;
  BufferPtr nulls_{nullptr};
  uint64_t* rawNulls_{nullptr};
};
} // namespace bytedance::bolt
