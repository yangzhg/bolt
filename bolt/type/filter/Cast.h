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

#include "bolt/type/NumericTypeUtils.h"
#include "bolt/type/Type.h"
#include "bolt/type/filter/FilterBase.h"
namespace bytedance::bolt::common {

template <TypeKind SOURCE, TypeKind TARGET>
class Cast final : public Filter, public IFilterWithInnerFilter {
 public:
  Cast(
      const TypePtr& sourceType,
      const TypePtr& targetType,
      std::shared_ptr<const Filter> innerFilter);

  Cast(
      const TypePtr& sourceType,
      const TypePtr& targetType,
      std::shared_ptr<const Filter> innerFilter,
      std::shared_ptr<const Filter> mergedNonCastFilter);

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const override;
  bool testingEquals(const Filter& other) const override;
  folly::dynamic serialize() const override;

  bool testInt64(int64_t value) const override;
  bool testDouble(double value) const override;
  bool testFloat(float value) const override;
  bool testInt64Range(int64_t min, int64_t max, bool hasNull) const override;
  bool testDoubleRange(double min, double max, bool hasNull) const override;

  bool testBool(bool value) const override;
  bool testBytes(const char* bytes, int32_t len) const override;
  bool testTimestamp(const Timestamp& timestamp) const override;
  bool testBytesRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const override;

  bool testNull() const override;
  bool testTimestampRange(Timestamp min, Timestamp max, bool hasNull)
      const override;
  bool hasTestLength() const override;

  std::unique_ptr<Filter> mergeWith(const Filter* other) const override;

  std::string toString() const override;

  const Filter* innerFilter() const override {
    return innerFilter_.get();
  }

  const TypePtr targetType() const override {
    return targetType_;
  }

  const TypePtr sourceType() const override {
    return sourceType_;
  }

  bool isNumericCast() const {
    return (isSourceIntegral || isSourceFloating) &&
        (isTargetIntegral || isTargetFloating);
  }

  bool requiresRangeAdjustment() const {
    return isSourceFloating && isTargetIntegral;
  }

 private:
  template <typename T>
  bool isInRange(double value) const {
    return value >= static_cast<double>(std::numeric_limits<T>::min()) &&
        value <= static_cast<double>(std::numeric_limits<T>::max()) &&
        !std::isnan(value) && !std::isinf(value);
  }

  bool handleIntegralRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const;

  bool handleFloatingRange(
      std::optional<std::string_view> min,
      std::optional<std::string_view> max,
      bool hasNull) const;

  template <typename T>
  bool isRangeInBounds(double min, double max) const {
    return isInRange<T>(min) && isInRange<T>(max);
  }

  const TypePtr sourceType_;
  const TypePtr targetType_;
  const std::shared_ptr<const Filter> innerFilter_;

  /// Stores a non-Cast filter that has been merged with this Cast filter.
  /// When testing values, we first check them against this filter (if present)
  /// using the original value type. Only values that pass this filter are then
  /// cast to the target type and tested against innerFilter_. This allows
  /// efficient combination of Cast filters with other filter types.
  std::shared_ptr<const Filter> mergedNonCastFilter_;

  static constexpr bool isSourceIntegral =
      type::NumericTypeUtils::isIntegralType(SOURCE);
  static constexpr bool isTargetIntegral =
      type::NumericTypeUtils::isIntegralType(TARGET);
  static constexpr bool isSourceFloating =
      type::NumericTypeUtils::isFloatingPointType(SOURCE);
  static constexpr bool isTargetFloating =
      type::NumericTypeUtils::isFloatingPointType(TARGET);

  template <typename T>
  std::enable_if_t<std::is_floating_point_v<T>, bool> testFloatingPoint(
      T value) const;
};

// Creates a Cast filter with appropriate template parameters based on source
// and target types and inner filter and merged none cast filter.
std::unique_ptr<Filter> createCastFilter(
    const TypePtr& sourceType,
    const TypePtr& targetType,
    std::shared_ptr<const Filter> innerFilter,
    std::shared_ptr<const Filter> mergedNonCastFilter);

// Creates a Cast filter with appropriate template parameters based on source
// and target types and inner filter.
std::unique_ptr<Filter> createCastFilter(
    const TypePtr& sourceType,
    const TypePtr& targetType,
    std::shared_ptr<const Filter> innerFilter);

std::unique_ptr<ISerializable> deserializeCastFilter(const folly::dynamic& obj);

// Template declarations
#define DECLARE_CAST_TEMPLATES(SOURCE_KIND)                     \
  extern template class Cast<SOURCE_KIND, TypeKind::TINYINT>;   \
  extern template class Cast<SOURCE_KIND, TypeKind::SMALLINT>;  \
  extern template class Cast<SOURCE_KIND, TypeKind::INTEGER>;   \
  extern template class Cast<SOURCE_KIND, TypeKind::BIGINT>;    \
  extern template class Cast<SOURCE_KIND, TypeKind::REAL>;      \
  extern template class Cast<SOURCE_KIND, TypeKind::DOUBLE>;    \
  extern template class Cast<SOURCE_KIND, TypeKind::VARCHAR>;   \
  extern template class Cast<SOURCE_KIND, TypeKind::VARBINARY>; \
  extern template class Cast<SOURCE_KIND, TypeKind::BOOLEAN>;   \
  extern template class Cast<SOURCE_KIND, TypeKind::TIMESTAMP>;

DECLARE_CAST_TEMPLATES(TypeKind::TINYINT)
DECLARE_CAST_TEMPLATES(TypeKind::SMALLINT)
DECLARE_CAST_TEMPLATES(TypeKind::INTEGER)
DECLARE_CAST_TEMPLATES(TypeKind::BIGINT)
DECLARE_CAST_TEMPLATES(TypeKind::REAL)
DECLARE_CAST_TEMPLATES(TypeKind::DOUBLE)
DECLARE_CAST_TEMPLATES(TypeKind::VARCHAR)
DECLARE_CAST_TEMPLATES(TypeKind::VARBINARY)
DECLARE_CAST_TEMPLATES(TypeKind::BOOLEAN)
DECLARE_CAST_TEMPLATES(TypeKind::TIMESTAMP)

#undef DECLARE_CAST_TEMPLATES
} // namespace bytedance::bolt::common
