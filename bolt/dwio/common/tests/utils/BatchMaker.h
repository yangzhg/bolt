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

#include <random>
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
namespace bytedance::bolt::test {

// sets child elements of null row/array/map to null. the result of
// 'a.b.c is null' can be determined just by looking at 'c' because
// a null parent 'b' is also null in 'c'. This makes generating
// testing filters simpler. Non-null content under a null container
// cannot be represented in file formats in any case.
void propagateNullsRecursive(BaseVector& vector);

struct BatchMaker {
  static VectorPtr createBatch(
      const TypePtr& type,
      uint64_t capacity,
      memory::MemoryPool& memoryPool,
      std::mt19937& gen,
      std::function<bool(vector_size_t /*index*/)> isNullAt = nullptr);

  static VectorPtr createBatch(
      const TypePtr& type,
      uint64_t capacity,
      memory::MemoryPool& memoryPool,
      std::function<bool(vector_size_t /*index*/)> isNullAt = nullptr,
      std::mt19937::result_type seed = std::mt19937::default_seed);

  template <TypeKind KIND>
  static VectorPtr createVector(
      const TypePtr& type,
      size_t size,
      memory::MemoryPool& pool,
      std::mt19937& gen,
      std::function<bool(vector_size_t /*index*/)> isNullAt = nullptr);

  template <TypeKind KIND>
  static VectorPtr createVector(
      const TypePtr& type,
      size_t size,
      memory::MemoryPool& pool,
      std::function<bool(vector_size_t /*index*/)> isNullAt = nullptr,
      std::mt19937::result_type seed = std::mt19937::default_seed) {
    std::mt19937 gen{seed};
    return createVector<KIND>(type, size, pool, gen, isNullAt);
  }
};

} // namespace bytedance::bolt::test
