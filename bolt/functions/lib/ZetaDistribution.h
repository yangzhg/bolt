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

#include <random>

#include "bolt/common/base/Exceptions.h"
namespace bytedance::bolt::functions {

/// Zeta distribution, the PMF is given by
///
/// \f[ P(X = k) = \frac{1}{k^s\zeta(s)} \f]
///
/// where k is positive integer and \f$ \zeta(s) \f$ is the Riemann zeta
/// function
///
/// \f[ \zeta(s) = \sum_{i=1}^n \frac{1}{i^s} \f]
///
/// This is mainly used to generate test data with skewed frequencies.
struct ZetaDistribution {
  ZetaDistribution(double s, int n) : cdf_(n) {
    BOLT_CHECK(s > 1 && n >= 1);
    double z = 0;
    for (int i = 1; i <= n; ++i) {
      z += pow(i, -s);
      cdf_[i - 1] = z;
    }
    cdf_.pop_back();
    for (double& p : cdf_) {
      p /= z;
    }
  }

  template <typename Generator>
  int operator()(Generator& g) {
    double z = uniform_(g);
    auto it = std::lower_bound(cdf_.begin(), cdf_.end(), z);
    return 1 + (it - cdf_.begin());
  }

 private:
  std::uniform_real_distribution<> uniform_{0, 1};
  std::vector<double> cdf_;
};

} // namespace bytedance::bolt::functions
