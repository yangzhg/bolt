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

#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::generator_spec_examples {

using Rng = std::mt19937;
using Sample =
    std::tuple<std::map<int32_t, size_t>, int32_t, int32_t>; // (histogram, null
                                                             // count, total
                                                             // count)

template <typename T>
Sample convertToSample(FlatVector<T>* flatVector) {
  Sample sample;
  auto& [hist, nulls, total] = sample;
  nulls = 0;
  total = flatVector->size();
  for (auto i = 0; i < flatVector->size(); ++i) {
    if (flatVector->isNullAt(i)) {
      ++nulls;
    } else {
      int32_t val = std::round(flatVector->valueAt(i));
      hist[val]++;
    }
  }
  return sample;
}

template <typename T>
std::string plotVector(FlatVector<T>* flatVector, const size_t norm = 200) {
  const auto& [hist, nulls, total] = convertToSample(flatVector);
  std::stringstream sstream;
  for (auto [val, num] : hist) {
    sstream << std::setw(2) << val << ' ' << std::string(num / norm, '*')
            << "\n";
  }
  sstream << "Null ratio = " << ((double)nulls / (double)total) << "\n";
  return sstream.str();
}

} // namespace bytedance::bolt::generator_spec_examples
