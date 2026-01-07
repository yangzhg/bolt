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

#include <memory>
#include <string>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/serialization/Serializable.h"
#include "bolt/type/Type.h"
#include "bolt/type/filter/FilterBase.h"
namespace bytedance::bolt::common {

template <typename T>
class FloatingPointRange;

template <typename T>
class FloatingPointMultiRange final : public Filter {
  static_assert(
      std::is_same_v<T, float> || std::is_same_v<T, double>,
      "T must be float or double");

 public:
  /// @param ranges List of range filters. Must contain at least two entries.
  /// All entries must support the same data types.
  /// @param nullAllowed Null values are passing the filter if true. nullAllowed
  /// flags in the 'ranges' filters are ignored.
  /// @param nanAllowed Not-a-Number floating point values are passing the
  /// filter if true. Applies to floating point data types only. NaN values are
  /// not further tested using contained filters.
  FloatingPointMultiRange(
      std::vector<std::unique_ptr<FloatingPointRange<T>>> ranges,
      bool nullAllowed);

  FloatingPointMultiRange(
      const FloatingPointMultiRange& other,
      bool nullAllowed);

  folly::dynamic serialize() const override;
  static FilterPtr create(const folly::dynamic& obj);
  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final;

  bool testDouble(double value) const final;
  bool testFloat(float value) const final;
  bool testDoubleRange(double min, double max, bool hasNull) const final;
  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;

  // Accessor methods
  const std::vector<std::unique_ptr<FloatingPointRange<T>>>& ranges() const {
    return ranges_;
  }

  std::string toString() const override;
  bool testingEquals(const Filter& other) const final;

 private:
  std::vector<std::unique_ptr<FloatingPointRange<T>>> cloneRanges() const;
  void validateRanges() const;
  std::vector<std::unique_ptr<FloatingPointRange<T>>> ranges_;
  std::vector<T> lowerBounds_; // Sorted lower bounds for binary search
};

using FloatMultiRange = FloatingPointMultiRange<float>;
using DoubleMultiRange = FloatingPointMultiRange<double>;

} // namespace bytedance::bolt::common