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

#include "bolt/vector/BaseVector.h"
#include "bolt/vector/NullsBuilder.h"
namespace bytedance::bolt {

using FuzzerGenerator = std::mt19937;

namespace generator_spec_utils {

bool coinToss(FuzzerGenerator& rng, double threshold);

vector_size_t getRandomIndex(FuzzerGenerator& rng, vector_size_t maxIndex);

BufferPtr generateNullsBuffer(
    FuzzerGenerator& rng,
    memory::MemoryPool* pool,
    vector_size_t vectorSize,
    double nullProbability);

BufferPtr generateIndicesBuffer(
    FuzzerGenerator& rng,
    memory::MemoryPool* pool,
    vector_size_t bufferSize,
    vector_size_t baseVectorSize);

} // namespace generator_spec_utils
} // namespace bytedance::bolt
