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
#include "bolt/type/filter/FloatingPointRange.h"
namespace bytedance::bolt::common {

template <typename T>
class NegatedFloatingPointRange final : public AbstractRange {
  static_assert(
      std::is_same_v<T, float> || std::is_same_v<T, double>,
      "T must be float or double");

 public:
  NegatedFloatingPointRange(
      T lower,
      bool lowerUnbounded,
      bool lowerExclusive,
      T upper,
      bool upperUnbounded,
      bool upperExclusive,
      bool nullAllowed);

  NegatedFloatingPointRange(
      const NegatedFloatingPointRange& other,
      bool nullAllowed);

  double lower() const {
    return inner_.lower();
  }

  double upper() const {
    return inner_.upper();
  }

  folly::dynamic serialize() const override;
  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const final;
  bool testDouble(double value) const final;
  bool testFloat(float value) const final;
  xsimd::batch_bool<double> testValues(xsimd::batch<double> values) const final;
  xsimd::batch_bool<float> testValues(xsimd::batch<float> values) const final;
  bool testDoubleRange(double min, double max, bool hasNull) const final;
  std::unique_ptr<Filter> mergeWith(const Filter* other) const final;
  std::string toString() const final;
  bool testingEquals(const Filter& other) const final;

  static FilterPtr create(const folly::dynamic& obj);

 private:
  const FloatingPointRange<T> inner_;
};

using NegatedDoubleRange = NegatedFloatingPointRange<double>;
using NegatedFloatRange = NegatedFloatingPointRange<float>;

extern template class NegatedFloatingPointRange<float>;
extern template class NegatedFloatingPointRange<double>;

} // namespace bytedance::bolt::common