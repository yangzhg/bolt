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
#include "bolt/functions/lib/aggregates/SingleValueAccumulator.h"
#include "bolt/vector/DecodedVector.h"
namespace bytedance::bolt::aggregate::prestosql {

/// Compare the new value of the DecodedVector at the given index with the value
/// stored in the SingleValueAccumulator. Returns 0 if stored and new values are
/// equal; <0 if stored value is less then new value; >0 if stored value is
/// greater than new value.
///
/// The default nullHandlingMode in Presto is StopAtNull so it will throw an
/// exception when complex type values contain nulls.
int32_t compare(
    const bolt::functions::aggregate::SingleValueAccumulator* accumulator,
    const DecodedVector& decoded,
    vector_size_t index);

inline int32_t compare(
    const bolt::functions::aggregate::SingleVarcharAccumulator* accumulator,
    const DecodedVector& decoded,
    vector_size_t index) {
  return accumulator->compare(decoded, index);
}
} // namespace bytedance::bolt::aggregate::prestosql
