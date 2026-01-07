/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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
 */

#pragma once

#include "bolt/core/QueryConfig.h"
#include "bolt/functions/Macros.h"
#include "folly/Random.h"
namespace bytedance::bolt::functions::flinksql {

template <typename TExec>
struct RandIntegerFunction {
  BOLT_DEFINE_FUNCTION_TYPES(TExec);

  static constexpr bool is_deterministic = false;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<int32_t>* /*max*/,
      const arg_type<int32_t>* seed) {
    generator_ = std::mt19937{};
    if (seed) {
      generator_.seed(*seed);
    }
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, int32_t max) {
    result = folly::Random::rand32(max);
  }

  FOLLY_ALWAYS_INLINE void
  call(int32_t& result, int32_t max, int32_t /*seed*/) {
    result = folly::Random::rand32(max, generator_);
  }

 private:
  std::mt19937 generator_;
};
} // namespace bytedance::bolt::functions::flinksql