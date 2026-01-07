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

#include "bolt/type/filter/FilterBase.h"
#include "bolt/type/filter/FloatingPointValues.h"
namespace bytedance::bolt::common {

template <typename T>
class NegatedFloatingPointValues final : public Filter {
 public:
  NegatedFloatingPointValues(
      const std::vector<T>& values,
      bool nullAllowed,
      bool nanAllowed = false);

  NegatedFloatingPointValues(
      const NegatedFloatingPointValues& other,
      bool nullAllowed);

  folly::dynamic serialize() const override;
  static FilterPtr create(const folly::dynamic& obj);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final;

  bool testDouble(double value) const final {
    return !nonNegated_->testDouble(value);
  }

  bool testFloat(float value) const final {
    return !nonNegated_->testFloat(value);
  }

  xsimd::batch_bool<double> testValues(
      xsimd::batch<double> values) const final {
    return ~nonNegated_->testValues(values);
  }

  xsimd::batch_bool<float> testValues(xsimd::batch<float> values) const final {
    return ~nonNegated_->testValues(values);
  }

  bool testDoubleRange(double min, double max, bool hasNull) const final;
  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;
  bool testingEquals(const Filter& other) const final;

  std::string toString() const final;

  // Accessor methods
  T min() const {
    return nonNegated_->min();
  }

  T max() const {
    return nonNegated_->max();
  }

  const std::set<T>& values() const {
    return nonNegated_->values();
  }

 private:
  std::unique_ptr<FloatingPointValues<T>> nonNegated_;
};

using NegatedDoubleValues = NegatedFloatingPointValues<double>;
using NegatedFloatValues = NegatedFloatingPointValues<float>;

extern template class NegatedFloatingPointValues<float>;
extern template class NegatedFloatingPointValues<double>;

} // namespace bytedance::bolt::common