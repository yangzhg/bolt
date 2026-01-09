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

#include <cmath>
#include <memory>
#include <set>
#include <vector>

#include "bolt/common/base/SimdUtil.h"
#include "bolt/type/filter/FilterBase.h"
namespace bytedance::bolt::common {

template <typename T>
class FloatingPointValues : public Filter {
 public:
  /// @param values A list of unique values that pass the filter. Must contain
  /// at least two entries.
  /// @param nullAllowed Null values are passing the filter if true.
  /// @param nanAllowed NaN values are passing the filter if true.
  /// comparison.
  FloatingPointValues(
      const std::vector<T>& values,
      bool nullAllowed,
      bool nanAllowed = false);

  // Copy constructor with optional nullAllowed override
  explicit FloatingPointValues(
      const FloatingPointValues& other,
      bool nullAllowed);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const override;
  bool testingEquals(const Filter& other) const override;
  folly::dynamic serialize() const override;
  static FilterPtr create(const folly::dynamic& obj);

  const std::set<T>& values() const {
    return values_;
  }

  T min() const {
    return min_;
  }

  T max() const {
    return max_;
  }

  bool testDouble(double value) const override;
  bool testFloat(float value) const override;
  bool testDoubleRange(double min, double max, bool hasNull) const override;
  std::unique_ptr<Filter> mergeWith(const Filter* other) const override;

  xsimd::batch_bool<double> testValues(
      xsimd::batch<double> values) const override;
  xsimd::batch_bool<float> testValues(
      xsimd::batch<float> values) const override;

  std::string toString() const override;

 private:
  bool testFloatingPoint(T value) const;

  std::set<T> values_;
  const T min_;
  const T max_;
  const bool nanAllowed_;
};

using DoubleValues = FloatingPointValues<double>;
using FloatValues = FloatingPointValues<float>;

extern template class FloatingPointValues<float>;
extern template class FloatingPointValues<double>;

} // namespace bytedance::bolt::common
