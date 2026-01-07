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

#include "bolt/common/file/Utils.h"
#include "bolt/common/base/Exceptions.h"
namespace bytedance::bolt::file::utils {

bool CoalesceIfDistanceLE::operator()(
    const bolt::common::Region& a,
    const bolt::common::Region& b) const {
  BOLT_CHECK_LE(a.offset, b.offset, "Regions to combine must be sorted.");
  const uint64_t beginGap = a.offset + a.length, endGap = b.offset;

  BOLT_CHECK_LE(beginGap, endGap, "Regions to combine can't overlap.");
  const uint64_t gap = endGap - beginGap;

  return gap <= maxCoalescingDistance_;
}

} // namespace bytedance::bolt::file::utils
