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

#include "bolt/functions/lib/KllSketch.h"
namespace bytedance::bolt::functions::kll {

uint32_t kFromEpsilon(double eps) {
  return ceil(exp(1.0285 * log(2.296 / eps)));
}

namespace detail {

namespace {

constexpr uint8_t kMinBufferWidth = 8;

double powerOfTwoThirds(int n) {
  static const auto kMemo = [] {
    std::array<double, kMaxLevel> memo;
    for (int i = 0; i < kMaxLevel; ++i) {
      memo[i] = pow(2.0 / 3.0, i);
    }
    return memo;
  }();
  return kMemo[n];
}

} // namespace

uint32_t computeTotalCapacity(uint32_t k, uint8_t numLevels) {
  uint32_t total = 0;
  for (uint8_t h = 0; h < numLevels; ++h) {
    total += levelCapacity(k, numLevels, h);
  }
  return total;
}

uint32_t levelCapacity(uint32_t k, uint8_t numLevels, uint8_t height) {
  BOLT_DCHECK_LT(height, numLevels);
  BOLT_DCHECK_LE(numLevels, kMaxLevel);
  return std::max<uint32_t>(
      kMinBufferWidth, k * powerOfTwoThirds(numLevels - height - 1));
}

uint8_t floorLog2(uint64_t p, uint64_t q) {
  for (uint8_t ans = 0;; ++ans) {
    q <<= 1;
    if (p < q) {
      return ans;
    }
  }
}

uint64_t sumSampleWeights(uint8_t numLevels, const uint32_t* levels) {
  uint64_t total = 0;
  uint64_t weight = 1;
  for (uint8_t lvl = 0; lvl < numLevels; lvl++) {
    total += weight * (levels[lvl + 1] - levels[lvl]);
    weight *= 2;
  }
  return total;
}

} // namespace detail
} // namespace bytedance::bolt::functions::kll
