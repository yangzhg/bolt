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

#include "bolt/type/filter/FloatingPointMultiRange.h"
#include <algorithm>
#include <sstream>
#include "bolt/type/filter/FilterUtil.h"
#include "bolt/type/filter/FloatingPointRange.h"
namespace bytedance::bolt::common {

template <typename T>
FloatingPointMultiRange<T>::FloatingPointMultiRange(
    std::vector<std::unique_ptr<FloatingPointRange<T>>> ranges,
    bool nullAllowed)
    : Filter(
          true,
          nullAllowed,
          (std::is_same_v<T, double>) ? FilterKind::kDoubleMultiRange
                                      : FilterKind::kFloatMultiRange),
      ranges_(std::move(ranges)) {
  // Sort ranges by lower bound
  std::sort(ranges_.begin(), ranges_.end(), [](const auto& a, const auto& b) {
    return a->lower() < b->lower();
  });

  validateRanges();

  // Maintain sorted lower bounds for binary search
  lowerBounds_.reserve(ranges_.size());
  for (const auto& range : ranges_) {
    lowerBounds_.push_back(range->lower());
  }
}

template <typename T>
FloatingPointMultiRange<T>::FloatingPointMultiRange(
    const FloatingPointMultiRange& other,
    bool nullAllowed)
    : Filter(
          true,
          nullAllowed,
          (std::is_same_v<T, double>) ? FilterKind::kDoubleMultiRange
                                      : FilterKind::kFloatMultiRange),
      lowerBounds_(other.lowerBounds_) {
  ranges_.reserve(other.ranges_.size());
  for (const auto& range : other.ranges_) {
    ranges_.push_back(std::unique_ptr<FloatingPointRange<T>>(
        static_cast<FloatingPointRange<T>*>(range->clone().release())));
  }
}

template <typename T>
void FloatingPointMultiRange<T>::validateRanges() const {
  BOLT_CHECK_GT(
      ranges_.size(), 1, "MultiRange must contain at least two ranges");

  // Ensure ranges are ordered and non-overlapping
  for (size_t i = 1; i < ranges_.size(); i++) {
    const auto& prev = ranges_[i - 1];
    const auto& curr = ranges_[i];

    // For ranges to not overlap:
    // - If prev's upper bound is exclusive OR curr's lower bound is exclusive,
    //   they can be equal
    // - Otherwise, prev's upper must be strictly less than curr's lower
    bool prevUpperExclusive = prev->upperExclusive();
    bool currLowerExclusive = curr->lowerExclusive();

    if (prevUpperExclusive || currLowerExclusive) {
      BOLT_CHECK_LE(
          prev->upper(),
          curr->lower(),
          "Ranges in FloatingPointMultiRange must be non-overlapping and ordered");
    } else {
      BOLT_CHECK_LT(
          prev->upper(),
          curr->lower(),
          "Ranges in FloatingPointMultiRange must be non-overlapping and ordered");
    }
  }
}

template <typename T>
std::vector<std::unique_ptr<FloatingPointRange<T>>>
FloatingPointMultiRange<T>::cloneRanges() const {
  std::vector<std::unique_ptr<FloatingPointRange<T>>> copy;
  copy.reserve(ranges_.size());
  for (const auto& range : ranges_) {
    copy.push_back(std::unique_ptr<FloatingPointRange<T>>(
        static_cast<FloatingPointRange<T>*>(range->clone().release())));
  }
  return copy;
}

template <typename T>
std::unique_ptr<Filter> FloatingPointMultiRange<T>::clone(
    std::optional<bool> nullAllowed) const {
  return std::make_unique<FloatingPointMultiRange<T>>(
      *this, nullAllowed.value_or(nullAllowed_));
}

template <typename T>
bool FloatingPointMultiRange<T>::testDouble(double value) const {
  if (std::isnan(value)) {
    return false;
  }
  auto it = std::lower_bound(lowerBounds_.begin(), lowerBounds_.end(), value);
  size_t index = std::distance(lowerBounds_.begin(), it);

  if (index > 0 && ranges_[index - 1]->testDouble(value)) {
    return true;
  }
  if (index < ranges_.size() && ranges_[index]->testDouble(value)) {
    return true;
  }
  return false;
}

template <typename T>
bool FloatingPointMultiRange<T>::testFloat(float value) const {
  if (std::isnan(value)) {
    return false;
  }
  auto it = std::lower_bound(
      lowerBounds_.begin(), lowerBounds_.end(), static_cast<T>(value));
  size_t index = std::distance(lowerBounds_.begin(), it);

  if (index > 0 && ranges_[index - 1]->testFloat(value)) {
    return true;
  }
  if (index < ranges_.size() && ranges_[index]->testFloat(value)) {
    return true;
  }
  return false;
}

template <typename T>
bool FloatingPointMultiRange<T>::testDoubleRange(
    double min,
    double max,
    bool hasNull) const {
  if (hasNull && nullAllowed_) {
    return true;
  }
  if (std::isnan(min) || std::isnan(max)) {
    return false;
  }
  for (const auto& range : ranges_) {
    if (range->testDoubleRange(min, max, false)) {
      return true;
    }
  }
  return false;
}

template <typename T>
std::unique_ptr<Filter> FloatingPointMultiRange<T>::mergeWith(
    const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return std::make_unique<FloatingPointMultiRange<T>>(*this, false);
    case FilterKind::kDoubleRange:
    case FilterKind::kFloatRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto rangesCopy = cloneRanges();
      auto otherRange = static_cast<const FloatingPointRange<T>*>(other);
      rangesCopy.push_back(std::unique_ptr<FloatingPointRange<T>>(
          static_cast<FloatingPointRange<T>*>(otherRange->clone().release())));
      return combineFloatRanges(std::move(rangesCopy), bothNullAllowed);
    }
    case FilterKind::kDoubleMultiRange:
    case FilterKind::kFloatMultiRange: {
      bool bothNullAllowed = nullAllowed_ && other->testNull();
      auto rangesCopy = cloneRanges();
      auto otherMulti = static_cast<const FloatingPointMultiRange<T>*>(other);
      for (const auto& range : otherMulti->ranges_) {
        rangesCopy.push_back(std::unique_ptr<FloatingPointRange<T>>(
            static_cast<FloatingPointRange<T>*>(range->clone().release())));
      }
      return combineFloatRanges(std::move(rangesCopy), bothNullAllowed);
    }
    default:
      return nullptr;
  }
}

template <typename T>
std::string FloatingPointMultiRange<T>::toString() const {
  std::ostringstream out;
  out << (std::is_same_v<T, double> ? "DoubleMultiRange" : "FloatMultiRange")
      << ": [";
  for (const auto& range : ranges_) {
    out << " " << range->toString();
  }
  out << " ]" << (nullAllowed_ ? " with nulls" : " no nulls");
  return out.str();
}

template <typename T>
bool FloatingPointMultiRange<T>::testingEquals(const Filter& other) const {
  auto otherMulti = dynamic_cast<const FloatingPointMultiRange<T>*>(&other);
  if (!otherMulti || !Filter::testingBaseEquals(other) ||
      ranges_.size() != otherMulti->ranges_.size()) {
    return false;
  }
  for (size_t i = 0; i < ranges_.size(); ++i) {
    if (!ranges_[i]->testingEquals(*otherMulti->ranges_[i])) {
      return false;
    }
  }
  return true;
}

template <typename T>
folly::dynamic FloatingPointMultiRange<T>::serialize() const {
  auto obj = Filter::serializeBase(
      std::is_same_v<T, double> ? "DoubleMultiRange" : "FloatMultiRange");
  folly::dynamic rangesArray = folly::dynamic::array;
  for (const auto& range : ranges_) {
    rangesArray.push_back(range->serialize());
  }
  obj["ranges"] = std::move(rangesArray);
  return obj;
}

template <typename T>
FilterPtr FloatingPointMultiRange<T>::create(const folly::dynamic& obj) {
  auto nullAllowed = deserializeNullAllowed(obj);
  std::vector<std::unique_ptr<FloatingPointRange<T>>> ranges;
  for (const auto& rangeObj : obj["ranges"]) {
    ranges.push_back(std::unique_ptr<FloatingPointRange<T>>(
        static_cast<FloatingPointRange<T>*>(
            FloatingPointRange<T>::create(rangeObj).release())));
  }
  return std::make_unique<FloatingPointMultiRange<T>>(
      std::move(ranges), nullAllowed);
}

// Explicit template instantiations
template class FloatingPointMultiRange<float>;
template class FloatingPointMultiRange<double>;

} // namespace bytedance::bolt::common