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

#include "bolt/functions/Macros.h"
namespace bytedance::bolt::functions::sparksql {

template <typename T>
struct RandFunction {
  static constexpr bool is_deterministic = false;

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const TInput* seedInput) {
    const auto partitionId = config.sparkPartitionId();
    int64_t seed = seedInput ? (int64_t)*seedInput : 0;
    generator_.seed(seed + partitionId);
  }

  FOLLY_ALWAYS_INLINE void call(double& result) {
    result = folly::Random::randDouble01();
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void callNullable(double& result, TInput /*seedInput*/) {
    result = folly::Random::randDouble01(generator_);
  }

 private:
  std::mt19937 generator_;
};
} // namespace bytedance::bolt::functions::sparksql
