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

#include "bolt/type/filter/NegatedFloatingPointRange.h"
namespace bytedance::bolt::common {

template <typename T>
NegatedFloatingPointRange<T>::NegatedFloatingPointRange(
    T lower,
    bool lowerUnbounded,
    bool lowerExclusive,
    T upper,
    bool upperUnbounded,
    bool upperExclusive,
    bool nullAllowed)
    : AbstractRange(
          lowerUnbounded,
          lowerExclusive,
          upperUnbounded,
          upperExclusive,
          nullAllowed,
          (std::is_same_v<T, double>) ? FilterKind::kNegatedDoubleRange
                                      : FilterKind::kNegatedFloatRange),
      inner_(
          lower,
          lowerUnbounded,
          lowerExclusive,
          upper,
          upperUnbounded,
          upperExclusive,
          !nullAllowed) {}

template <typename T>
NegatedFloatingPointRange<T>::NegatedFloatingPointRange(
    const NegatedFloatingPointRange& other,
    bool nullAllowed)
    : AbstractRange(
          other.lowerUnbounded_,
          other.lowerExclusive_,
          other.upperUnbounded_,
          other.upperExclusive_,
          nullAllowed,
          (std::is_same_v<T, double>) ? FilterKind::kNegatedDoubleRange
                                      : FilterKind::kNegatedFloatRange),
      inner_(other.inner_, !nullAllowed) {}

template <typename T>
std::unique_ptr<Filter> NegatedFloatingPointRange<T>::clone(
    std::optional<bool> nullAllowed) const {
  return std::make_unique<NegatedFloatingPointRange<T>>(
      *this, nullAllowed.value_or(nullAllowed_));
}

template <typename T>
bool NegatedFloatingPointRange<T>::testDouble(double value) const {
  return !inner_.testDouble(value);
}

template <typename T>
bool NegatedFloatingPointRange<T>::testFloat(float value) const {
  return !inner_.testFloat(value);
}

template <typename T>
xsimd::batch_bool<double> NegatedFloatingPointRange<T>::testValues(
    xsimd::batch<double> values) const {
  return !inner_.testValues(values);
}

template <typename T>
xsimd::batch_bool<float> NegatedFloatingPointRange<T>::testValues(
    xsimd::batch<float> values) const {
  return !inner_.testValues(values);
}

template <typename T>
bool NegatedFloatingPointRange<T>::testDoubleRange(
    double min,
    double max,
    bool hasNull) const {
  // If inner range is completely disjoint with [min, max], then negated range
  // completely contains [min, max]
  if (!inner_.testDoubleRange(min, max, hasNull)) {
    return true;
  }

  // If inner range partially overlaps with [min, max], then negated range also
  // partially overlaps
  if (min < inner_.lower() || max > inner_.upper()) {
    return true;
  }

  // If null is allowed and present, the range may pass
  if (hasNull && nullAllowed_) {
    return true;
  }

  // Inner range completely contains [min, max], so negated range is disjoint
  return false;
}

template <typename T>
std::unique_ptr<Filter> NegatedFloatingPointRange<T>::mergeWith(
    const Filter* other) const {
  switch (other->kind()) {
    case FilterKind::kAlwaysTrue:
    case FilterKind::kAlwaysFalse:
    case FilterKind::kIsNull:
      return other->mergeWith(this);
    case FilterKind::kIsNotNull:
      return std::make_unique<NegatedFloatingPointRange<T>>(
          inner_.lower(),
          inner_.lowerUnbounded(),
          inner_.lowerExclusive(),
          inner_.upper(),
          inner_.upperUnbounded(),
          inner_.upperExclusive(),
          false);
    case FilterKind::kNegatedDoubleRange:
    case FilterKind::kNegatedFloatRange: {
      if (this->kind() == other->kind()) {
        auto otherRange =
            static_cast<const NegatedFloatingPointRange<T>*>(other);

        // For negated ranges A' and B', their intersection is (A ∪ B)'
        // So we need to merge the inner ranges with OR semantics
        // If inner ranges can't be merged, the negated ranges can't be merged
        auto mergedInner = inner_.mergeWith(&otherRange->inner_);
        if (mergedInner != nullptr) {
          // The merged inner filter must be a FloatingPointRange
          auto* fpRange =
              dynamic_cast<FloatingPointRange<T>*>(mergedInner.get());
          if (fpRange != nullptr) {
            // Create new negated range with the merged inner range's bounds
            return std::make_unique<NegatedFloatingPointRange<T>>(
                fpRange->lower(),
                fpRange->lowerUnbounded(),
                fpRange->lowerExclusive(),
                fpRange->upper(),
                fpRange->upperUnbounded(),
                fpRange->upperExclusive(),
                nullAllowed_ && otherRange->nullAllowed_);
          }
        }
      }
      return nullptr;
    }
    default:
      return nullptr;
  }
}

template <typename T>
std::string NegatedFloatingPointRange<T>::toString() const {
  return fmt::format(
      "Negated{}: {}{}, {}{} {}",
      std::is_same_v<T, double> ? "DoubleRange" : "FloatRange",
      (lowerExclusive_ || lowerUnbounded_) ? "(" : "[",
      lowerUnbounded_ ? "-inf" : std::to_string(inner_.lower()),
      upperUnbounded_ ? "+inf" : std::to_string(inner_.upper()),
      (upperExclusive_ || upperUnbounded_) ? ")" : "]",
      nullAllowed_ ? "with nulls" : "no nulls");
}

template <typename T>
bool NegatedFloatingPointRange<T>::testingEquals(const Filter& other) const {
  if (kind() != other.kind()) {
    return false;
  }
  auto otherRange = static_cast<const NegatedFloatingPointRange<T>&>(other);
  return inner_.testingEquals(otherRange.inner_) &&
      nullAllowed_ == otherRange.nullAllowed_;
}

template <typename T>
folly::dynamic NegatedFloatingPointRange<T>::serialize() const {
  auto obj = AbstractRange::serializeBase(
      std::is_same_v<T, double> ? "NegatedDoubleRange" : "NegatedFloatRange");
  obj["lower"] = inner_.lower();
  obj["upper"] = inner_.upper();
  return obj;
}

template <typename T>
FilterPtr NegatedFloatingPointRange<T>::create(const folly::dynamic& obj) {
  bool nullAllowed = obj["nullAllowed"].getBool();
  double lower = obj["lower"].getDouble();
  double upper = obj["upper"].getDouble();
  bool lowerUnbounded = obj["lowerUnbounded"].getBool();
  bool upperUnbounded = obj["upperUnbounded"].getBool();
  bool lowerExclusive = obj["lowerExclusive"].getBool();
  bool upperExclusive = obj["upperExclusive"].getBool();

  return std::make_unique<NegatedFloatingPointRange<T>>(
      lower,
      lowerUnbounded,
      lowerExclusive,
      upper,
      upperUnbounded,
      upperExclusive,
      nullAllowed);
}

template class NegatedFloatingPointRange<float>;
template class NegatedFloatingPointRange<double>;

} // namespace bytedance::bolt::common
