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

#include "bolt/type/filter/FloatingPointValues.h"
#include <set>
#include <sstream>
#include "bolt/type/filter/FloatingPointRange.h"
namespace bytedance::bolt::common {

template <typename T>
FloatingPointValues<T>::FloatingPointValues(
    const std::vector<T>& values,
    bool nullAllowed,
    bool nanAllowed)
    : Filter(
          true,
          nullAllowed,
          (std::is_same_v<T, double>) ? FilterKind::kDoubleValues
                                      : FilterKind::kFloatValues),
      values_(values.begin(), values.end()),
      min_(*values_.begin()),
      max_(*values_.rbegin()),
      nanAllowed_(nanAllowed) {
  BOLT_CHECK(!std::isnan(min_), "min value cannot be NaN");
  BOLT_CHECK(!std::isnan(max_), "max value cannot be NaN");
  BOLT_CHECK(min_ < max_, "min must be less than max");
  BOLT_CHECK(!values.empty(), "values must not be empty");
}

template <typename T>
FloatingPointValues<T>::FloatingPointValues(
    const FloatingPointValues& other,
    bool nullAllowed)
    : Filter(
          true,
          nullAllowed,
          (std::is_same_v<T, double>) ? FilterKind::kDoubleValues
                                      : FilterKind::kFloatValues),
      values_(other.values_),
      min_(other.min_),
      max_(other.max_),
      nanAllowed_(other.nanAllowed_) {}

template <typename T>
std::unique_ptr<Filter> FloatingPointValues<T>::clone(
    std::optional<bool> nullAllowed) const {
  if (nullAllowed.has_value()) {
    return std::make_unique<FloatingPointValues<T>>(*this, nullAllowed.value());
  }
  return std::make_unique<FloatingPointValues<T>>(*this, nullAllowed_);
}

template <typename T>
bool FloatingPointValues<T>::testingEquals(const Filter& other) const {
  if (kind() != other.kind()) {
    return false;
  }
  auto otherValues = dynamic_cast<const FloatingPointValues<T>*>(&other);
  return nullAllowed_ == otherValues->nullAllowed_ &&
      values_ == otherValues->values_ &&
      nanAllowed_ == otherValues->nanAllowed_;
}

template <typename T>
folly::dynamic FloatingPointValues<T>::serialize() const {
  auto obj = Filter::serializeBase(
      std::is_same_v<T, double> ? "DoubleValues" : "FloatValues");
  folly::dynamic valuesArray = folly::dynamic::array;
  for (const auto& value : values_) {
    valuesArray.push_back(value);
  }
  obj["values"] = valuesArray;
  obj["nanAllowed"] = nanAllowed_;
  obj["nullAllowed"] = nullAllowed_;
  return obj;
}

template <typename T>
FilterPtr FloatingPointValues<T>::create(const folly::dynamic& obj) {
  BOLT_CHECK(obj.isObject());

  auto nanAllowed = obj["nanAllowed"].asBool();
  auto nullAllowed = obj["nullAllowed"].asBool();

  std::vector<T> values;
  for (const auto& value : obj["values"]) {
    values.push_back(value.asDouble());
  }

  return std::make_unique<FloatingPointValues<T>>(
      values, nullAllowed, nanAllowed);
}

template <typename T>
bool FloatingPointValues<T>::testFloatingPoint(T value) const {
  if (std::isnan(value)) {
    return nanAllowed_;
  }
  return values_.find(value) != values_.end();
}

template <typename T>
bool FloatingPointValues<T>::testDouble(double value) const {
  if constexpr (std::is_same_v<T, double>) {
    return testFloatingPoint(value);
  }
  return testFloatingPoint(static_cast<T>(value));
}

template <typename T>
bool FloatingPointValues<T>::testFloat(float value) const {
  if constexpr (std::is_same_v<T, float>) {
    return testFloatingPoint(value);
  }
  return testFloatingPoint(static_cast<T>(value));
}

template <typename T>
xsimd::batch_bool<double> FloatingPointValues<T>::testValues(
    xsimd::batch<double> values) const {
  return Filter::testValues(values);
}

template <typename T>
xsimd::batch_bool<float> FloatingPointValues<T>::testValues(
    xsimd::batch<float> values) const {
  return Filter::testValues(values);
}

template <typename T>
std::string FloatingPointValues<T>::toString() const {
  std::ostringstream out;
  out << (std::is_same_v<T, double> ? "DoubleValues" : "FloatValues") << ": [";
  bool first = true;
  for (const auto& value : values_) {
    if (!first) {
      out << ", ";
    }
    out << value;
    first = false;
  }
  out << "] " << (nullAllowed_ ? "with nulls" : "no nulls");
  if (nanAllowed_) {
    out << " with NaN";
  }
  return out.str();
}

template <typename T>
bool FloatingPointValues<T>::testDoubleRange(
    double min,
    double max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }

  if (std::isnan(min) || std::isnan(max)) {
    return nanAllowed_;
  }

  // Check if any value in the set falls within the range
  auto it = values_.lower_bound(static_cast<T>(min));
  return it != values_.end() && static_cast<double>(*it) <= max;
}

template <typename T>
std::unique_ptr<Filter> FloatingPointValues<T>::mergeWith(
    const Filter* other) const {
  if (!other) {
    return nullptr;
  }

  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);

    case FilterKind::kIsNotNull:
      return std::make_unique<FloatingPointValues<T>>(*this, false);

    case FilterKind::kDoubleRange:
    case FilterKind::kFloatRange: {
      auto rng = std::make_unique<FloatingPointRange<T>>(
          min_,
          false, /*lowerUnbounded*/
          false, /*includeLower*/
          max_,
          false, /*upperUnbounded*/
          false, /*includeUpper*/
          nullAllowed_);
      return rng->mergeWith(other);
    }

    case FilterKind::kDoubleValues:
    case FilterKind::kFloatValues: {
      const auto* that = dynamic_cast<const FloatingPointValues<T>*>(other);
      if (!that) {
        return nullptr;
      }

      auto min = std::max(min_, that->min_);
      auto max = std::min(max_, that->max_);

      if (min > max) {
        return nullptr;
      }

      bool newNullAllowed = nullAllowed_ && that->nullAllowed_;
      bool newNanAllowed = nanAllowed_ && that->nanAllowed_;

      std::vector<T> newValues;
      // Get intersection of values in the overlapping range
      for (const auto& value : values_) {
        if (value >= min && value <= max &&
            std::find(that->values_.begin(), that->values_.end(), value) !=
                that->values_.end()) {
          newValues.push_back(value);
        }
      }

      if (newValues.size() < 2) {
        return std::make_unique<FloatingPointRange<T>>(
            min,
            false, /*lowerUnbounded*/
            false, /*includeLower*/
            max,
            false, /*upperUnbounded*/
            false, /*includeUpper*/
            newNullAllowed);
      }

      return std::make_unique<FloatingPointValues<T>>(
          newValues, newNullAllowed, newNanAllowed);
    }

    default:
      return nullptr;
  }
}

// Explicit template instantiations
template class FloatingPointValues<float>;
template class FloatingPointValues<double>;

} // namespace bytedance::bolt::common