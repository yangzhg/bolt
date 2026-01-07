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
#include <vector>
namespace bytedance::bolt::common::hll {
// These tables come from Presto's HLL implementation:
// https://github.com/airlift/airlift/blob/master/stats/src/main/java/io/airlift/stats/cardinality/BiasCorrection.java
//
// These tables are generated empirically by running several thousand
// experiments and computing the average error for each cardinality
//
// See HyperLogLog in Practice: Algorithmic Engineering of a State of The Art
// Cardinality Estimation Algorithm paper at
// https://stefanheule.com/papers/edbt13-hyperloglog.pdf
class BiasCorrection {
 public:
  static const std::vector<std::vector<double>> kRawEstimates;
  static const std::vector<std::vector<double>> kBias;
};
} // namespace bytedance::bolt::common::hll
