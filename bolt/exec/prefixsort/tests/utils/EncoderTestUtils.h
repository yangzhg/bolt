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

#include "bolt/exec/prefixsort/PrefixSortEncoder.h"
namespace bytedance::bolt::exec::prefixsort::test {

/// Replace the elements in data with encoded ones(compatible with sorting in
/// ascending order), assuming that the elements in data are all non-null.
void encodeInPlace(std::vector<int64_t>& data);

/// Replace the elements in data with decoded ones, assuming that the elements
/// in data are all non-null and encoded(compatible with sorting in ascending
/// order).
void decodeInPlace(std::vector<int64_t>& data);

} // namespace bytedance::bolt::exec::prefixsort::test
