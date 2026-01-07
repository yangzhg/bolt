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

#include "bolt/common/base/BloomFilter.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/functions/Macros.h"
namespace bytedance::bolt::functions::sparksql {

template <typename T>
struct BloomFilterMightContainFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  using Allocator = std::allocator<uint64_t>;

  void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig&,
      const arg_type<Varbinary>* serialized,
      const arg_type<int64_t>*) {
    if (serialized != nullptr) {
      bloomFilter_.merge(serialized->str().c_str());
    }
  }

  FOLLY_ALWAYS_INLINE void
  call(bool& result, const arg_type<Varbinary>&, const int64_t& input) {
    result = bloomFilter_.isSet()
        ? bloomFilter_.mayContain(folly::hasher<int64_t>()(input))
        : false;
  }

 private:
  BloomFilter<Allocator> bloomFilter_;
};

} // namespace bytedance::bolt::functions::sparksql
