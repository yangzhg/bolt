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

#include <common/base/BoltException.h>
#include <gtest/gtest.h>
#include "bolt/type/filter/FloatingPointMultiRange.h"
#include "bolt/type/filter/FloatingPointRange.h"
#include "bolt/type/filter/FloatingPointValues.h"
#include "bolt/type/filter/NegatedFloatingPointRange.h"
#include "bolt/type/filter/NegatedFloatingPointValues.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::common;

class FloatingPointFiltersTest : public testing::Test {
 protected:
  void SetUp() override {}
};

TEST_F(FloatingPointFiltersTest, FloatingPointRangeBasic) {
  // Test basic range functionality
  FloatRange range(1.0f, false, false, 5.0f, false, false, true);

  EXPECT_TRUE(range.testFloat(1.0f));
  EXPECT_TRUE(range.testFloat(3.0f));
  EXPECT_TRUE(range.testFloat(5.0f));
  EXPECT_FALSE(range.testFloat(0.9f));
  EXPECT_FALSE(range.testFloat(5.1f));
  EXPECT_TRUE(range.testNull());
}

TEST_F(FloatingPointFiltersTest, FloatingPointRangeExclusive) {
  // Test exclusive bounds
  FloatRange range(1.0f, false, true, 5.0f, false, true, false);

  EXPECT_FALSE(range.testFloat(1.0f));
  EXPECT_TRUE(range.testFloat(1.1f));
  EXPECT_TRUE(range.testFloat(4.9f));
  EXPECT_FALSE(range.testFloat(5.0f));
  EXPECT_FALSE(range.testNull());
}

TEST_F(FloatingPointFiltersTest, FloatingPointRangeNaN) {
  FloatRange range(-1.0f, false, false, 1.0f, false, false, false);
  EXPECT_FALSE(range.testFloat(std::numeric_limits<float>::quiet_NaN()));
}

TEST_F(FloatingPointFiltersTest, FloatingPointRangeClone) {
  auto original = std::make_unique<FloatRange>(
      1.0f, false, false, 5.0f, false, false, true);
  auto cloned = original->clone();

  EXPECT_TRUE(original->testingEquals(*cloned));
  EXPECT_TRUE(cloned->testFloat(3.0f));
}

TEST_F(FloatingPointFiltersTest, FloatingPointMultiRangeBasic) {
  std::vector<std::unique_ptr<FloatingPointRange<float>>> ranges;
  ranges.push_back(std::make_unique<FloatRange>(
      1.0f, false, false, 2.0f, false, false, false));
  ranges.push_back(std::make_unique<FloatRange>(
      4.0f, false, false, 5.0f, false, false, false));

  FloatMultiRange multiRange(std::move(ranges), true);

  EXPECT_TRUE(multiRange.testFloat(1.5f));
  EXPECT_TRUE(multiRange.testFloat(4.5f));
  EXPECT_FALSE(multiRange.testFloat(3.0f));
  EXPECT_TRUE(multiRange.testNull());
}

TEST_F(FloatingPointFiltersTest, FloatingPointValuesBasic) {
  std::vector<double> values = {1.0, 2.0, 3.0, 4.0};
  DoubleValues filter(values, true);

  EXPECT_TRUE(filter.testDouble(1.0));
  EXPECT_TRUE(filter.testDouble(4.0));
  EXPECT_FALSE(filter.testDouble(1.5));
  EXPECT_FALSE(filter.testDouble(5.0));
  EXPECT_TRUE(filter.testNull());
}

TEST_F(FloatingPointFiltersTest, NegatedFloatingPointRangeBasic) {
  NegatedFloatRange range(1.0f, false, false, 5.0f, false, false, true);

  EXPECT_FALSE(range.testFloat(3.0f));
  EXPECT_TRUE(range.testFloat(0.9f));
  EXPECT_TRUE(range.testFloat(5.1f));
  EXPECT_TRUE(range.testNull());
}

TEST_F(FloatingPointFiltersTest, NegatedFloatingPointValuesBasic) {
  std::vector<double> values = {1.0, 2.0, 3.0};
  NegatedDoubleValues filter(values, true);

  EXPECT_FALSE(filter.testDouble(1.0));
  EXPECT_FALSE(filter.testDouble(2.0));
  EXPECT_TRUE(filter.testDouble(1.5));
  EXPECT_TRUE(filter.testDouble(4.0));
  EXPECT_TRUE(filter.testNull());
}

TEST_F(FloatingPointFiltersTest, FloatingPointRangeMerge) {
  auto range1 = std::make_unique<FloatRange>(
      1.0f, false, false, 3.0f, false, false, true);
  auto range2 = std::make_unique<FloatRange>(
      2.0f, false, false, 4.0f, false, false, true);

  auto merged = range1->mergeWith(range2.get());
  ASSERT_NE(merged, nullptr);

  EXPECT_TRUE(merged->testFloat(2.0f));
  EXPECT_TRUE(merged->testFloat(3.0f));
  EXPECT_FALSE(merged->testFloat(0.9f));
  EXPECT_FALSE(merged->testFloat(4.1f));
  EXPECT_TRUE(merged->testNull());
}

TEST_F(FloatingPointFiltersTest, FloatingPointRangeDoubleRange) {
  FloatRange range(1.0f, false, false, 5.0f, false, false, true);

  // Test range contains
  EXPECT_TRUE(range.testDoubleRange(2.0, 4.0, false));
  EXPECT_TRUE(range.testDoubleRange(0.0, 6.0, false));
  EXPECT_FALSE(range.testDoubleRange(5.1, 6.0, false));
  EXPECT_TRUE(range.testDoubleRange(5.1, 6.0, true)); // Due to nullAllowed
}

TEST_F(FloatingPointFiltersTest, NegatedRangeMerge) {
  auto range1 = std::make_unique<NegatedFloatRange>(
      1.0f, false, false, 3.0f, false, false, true);
  auto range2 = std::make_unique<NegatedFloatRange>(
      2.0f, false, false, 4.0f, false, false, true);

  auto merged = range1->mergeWith(range2.get());
  ASSERT_NE(merged, nullptr);

  // The merged result should exclude the intersection of the original ranges
  EXPECT_TRUE(merged->testFloat(0.9f));
  EXPECT_FALSE(merged->testFloat(2.5f));
  EXPECT_TRUE(merged->testFloat(4.1f));
}

TEST_F(FloatingPointFiltersTest, EdgeCases) {
  // Test infinity and very large/small numbers
  FloatRange range(
      -std::numeric_limits<float>::infinity(),
      false, // not unbounded, using actual -infinity value
      false, // inclusive
      std::numeric_limits<float>::infinity(),
      false, // not unbounded, using actual +infinity value
      false, // inclusive
      false);

  // Test various edge cases
  EXPECT_TRUE(range.testFloat(std::numeric_limits<float>::lowest()));
  EXPECT_TRUE(range.testFloat(0.0f));
  EXPECT_TRUE(range.testFloat(std::numeric_limits<float>::max()));
  EXPECT_TRUE(range.testFloat(-std::numeric_limits<float>::infinity()));
  EXPECT_TRUE(range.testFloat(std::numeric_limits<float>::infinity()));
  EXPECT_FALSE(range.testFloat(std::numeric_limits<float>::quiet_NaN()));
}

// Add more test cases for unbounded ranges
TEST_F(FloatingPointFiltersTest, UnboundedRanges) {
  // Test lower unbounded range
  FloatRange lowerUnbounded(
      0.0f, // value doesn't matter when unbounded
      true, // lowerUnbounded
      false, // lowerExclusive
      1.0f, // upper
      false, // upperUnbounded
      false, // upperExclusive
      false);

  EXPECT_TRUE(lowerUnbounded.testFloat(std::numeric_limits<float>::lowest()));
  EXPECT_TRUE(
      lowerUnbounded.testFloat(-std::numeric_limits<float>::infinity()));
  EXPECT_TRUE(lowerUnbounded.testFloat(0.5f));
  EXPECT_FALSE(lowerUnbounded.testFloat(1.5f));

  // Test upper unbounded range
  FloatRange upperUnbounded(
      1.0f, // lower
      false, // lowerUnbounded
      false, // lowerExclusive
      0.0f, // value doesn't matter when unbounded
      true, // upperUnbounded
      false, // upperExclusive
      false);

  EXPECT_FALSE(upperUnbounded.testFloat(0.5f));
  EXPECT_TRUE(upperUnbounded.testFloat(1.5f));
  EXPECT_TRUE(upperUnbounded.testFloat(std::numeric_limits<float>::max()));
  EXPECT_TRUE(upperUnbounded.testFloat(std::numeric_limits<float>::infinity()));
}

TEST_F(FloatingPointFiltersTest, MultiRangeMerge) {
  std::vector<std::unique_ptr<FloatingPointRange<float>>> ranges1;
  ranges1.push_back(std::make_unique<FloatRange>(
      1.0f, false, false, 2.0f, false, false, false));
  ranges1.push_back(std::make_unique<FloatRange>(
      4.0f, false, false, 5.0f, false, false, false));
  FloatMultiRange multiRange1(std::move(ranges1), true);

  std::vector<std::unique_ptr<FloatingPointRange<float>>> ranges2;
  ranges2.push_back(std::make_unique<FloatRange>(
      1.5f, false, false, 2.5f, false, false, false));
  ranges2.push_back(std::make_unique<FloatRange>(
      3.5f, false, false, 4.5f, false, false, false));
  FloatMultiRange multiRange2(std::move(ranges2), true);

  auto merged = multiRange1.mergeWith(&multiRange2);
  ASSERT_NE(merged, nullptr);

  EXPECT_TRUE(merged->testFloat(1.7f)); // In overlapping region
  EXPECT_FALSE(merged->testFloat(3.0f)); // Outside both ranges
  EXPECT_TRUE(merged->testFloat(4.2f)); // In overlapping region
  EXPECT_TRUE(merged->testNull());
}

TEST_F(FloatingPointFiltersTest, PrecisionEdgeCases) {
  // Test with values very close to each other
  double epsilon = std::numeric_limits<double>::epsilon();
  DoubleRange range(1.0, false, false, 1.0 + epsilon, false, false, false);

  EXPECT_TRUE(range.testDouble(1.0));
  EXPECT_TRUE(range.testDouble(1.0 + epsilon));
  EXPECT_FALSE(range.testDouble(1.0 + 2 * epsilon));

  // Test with denormalized numbers
  double denorm = std::numeric_limits<double>::denorm_min();
  DoubleRange denormRange(
      denorm, false, false, denorm * 2, false, false, false);
  EXPECT_TRUE(denormRange.testDouble(denorm));
  EXPECT_FALSE(denormRange.testDouble(0.0));
}

TEST_F(FloatingPointFiltersTest, ValueSetOperations) {
  // Test complex value set operations
  std::vector<double> values1 = {1.0, 2.0, 3.0, 4.0};
  std::vector<double> values2 = {2.0, 3.0, 4.0, 5.0};

  DoubleValues filter1(values1, true);
  DoubleValues filter2(values2, false);

  auto merged = filter1.mergeWith(&filter2);
  ASSERT_NE(merged, nullptr);

  // Test merged results
  EXPECT_FALSE(merged->testDouble(1.0)); // Only in first set
  EXPECT_TRUE(merged->testDouble(3.0)); // In both sets
  EXPECT_FALSE(merged->testDouble(5.0)); // Only in second set
  EXPECT_FALSE(merged->testDouble(6.0)); // In neither set
}

TEST_F(FloatingPointFiltersTest, FloatingPointRangeCornerCases) {
  // Test various corner cases
  FloatRange range(
      std::numeric_limits<float>::lowest(),
      false,
      true,
      std::numeric_limits<float>::max(),
      false,
      true,
      true);

  // Test extreme values
  EXPECT_FALSE(
      range.testFloat(std::numeric_limits<float>::lowest())); // Exclusive bound
  EXPECT_TRUE(range.testFloat(0.0f));
  EXPECT_FALSE(
      range.testFloat(std::numeric_limits<float>::max())); // Exclusive bound
  EXPECT_FALSE(range.testFloat(std::numeric_limits<float>::infinity()));
  EXPECT_FALSE(range.testFloat(-std::numeric_limits<float>::infinity()));
  EXPECT_TRUE(range.testNull());
}

TEST_F(FloatingPointFiltersTest, MultiRangeNonOverlapping) {
  std::vector<std::unique_ptr<FloatingPointRange<double>>> ranges;

  // Add non-overlapping ranges in order
  ranges.push_back(std::make_unique<DoubleRange>(
      -1.0,
      false,
      true, // (-1.0
      0.0,
      false,
      true, // ,0.0)
      false));

  ranges.push_back(std::make_unique<DoubleRange>(
      1.0,
      false,
      true, // (1.0
      2.0,
      false,
      true, // ,2.0)
      false));

  // This should construct successfully
  DoubleMultiRange multiRange(std::move(ranges), false);

  // Test various points
  EXPECT_FALSE(multiRange.testDouble(-1.0)); // Exclusive bound
  EXPECT_TRUE(multiRange.testDouble(-0.5)); // In first range
  EXPECT_FALSE(multiRange.testDouble(0.0)); // Exclusive bound
  EXPECT_FALSE(multiRange.testDouble(0.5)); // Between ranges
  EXPECT_FALSE(multiRange.testDouble(1.0)); // Exclusive bound
  EXPECT_TRUE(multiRange.testDouble(1.5)); // In second range
  EXPECT_FALSE(multiRange.testDouble(2.0)); // Exclusive bound
}

TEST_F(FloatingPointFiltersTest, MultiRangeValidationFailure) {
  // Test overlapping ranges - should fail construction
  std::vector<std::unique_ptr<FloatingPointRange<double>>> ranges;

  ranges.push_back(std::make_unique<DoubleRange>(
      0.0,
      false, // not unbounded
      true,
      2.0,
      false, // not unbounded
      true,
      false));

  ranges.push_back(std::make_unique<DoubleRange>(
      1.0,
      false, // not unbounded
      true,
      3.0,
      false, // not unbounded
      true,
      false));

  // Should throw exception due to overlapping ranges
  EXPECT_THROW(DoubleMultiRange(std::move(ranges), false), BoltRuntimeError);
}

// Add more validation failure cases
TEST_F(FloatingPointFiltersTest, MultiRangeValidationFailureVariants) {
  // Case 1: Adjacent ranges with inclusive bounds (overlap at boundary)
  {
    std::vector<std::unique_ptr<FloatingPointRange<double>>> ranges;
    ranges.push_back(std::make_unique<DoubleRange>(
        0.0,
        false,
        false, // [0.0
        1.0,
        false,
        false, // ,1.0]
        false));
    ranges.push_back(std::make_unique<DoubleRange>(
        1.0,
        false,
        false, // [1.0
        2.0,
        false,
        false, // ,2.0]
        false));
    EXPECT_THROW(DoubleMultiRange(std::move(ranges), false), BoltRuntimeError);
  }

  // Case 2: Ranges where second range is entirely within first
  {
    std::vector<std::unique_ptr<FloatingPointRange<double>>> ranges;
    ranges.push_back(std::make_unique<DoubleRange>(
        0.0,
        false,
        false, // [0.0
        3.0,
        false,
        false, // ,3.0]
        false));
    ranges.push_back(std::make_unique<DoubleRange>(
        1.0,
        false,
        false, // [1.0
        2.0,
        false,
        false, // ,2.0]
        false));
    EXPECT_THROW(DoubleMultiRange(std::move(ranges), false), BoltRuntimeError);
  }

  // Case 3: Adjacent ranges with non-matching exclusive/inclusive bounds
  {
    std::vector<std::unique_ptr<FloatingPointRange<double>>> ranges;
    ranges.push_back(std::make_unique<DoubleRange>(
        0.0,
        false,
        false, // [0.0
        1.0,
        false,
        true, // ,1.0)
        false));
    ranges.push_back(std::make_unique<DoubleRange>(
        1.0,
        false,
        true, // (1.0
        2.0,
        false,
        false, // ,2.0]
        false));
    DoubleMultiRange(std::move(ranges), false);
  }
}

TEST_F(FloatingPointFiltersTest, MultiRangeSpecialValues) {
  std::vector<std::unique_ptr<FloatingPointRange<double>>> ranges;

  // Add non-overlapping ranges with special values
  ranges.push_back(std::make_unique<DoubleRange>(
      -std::numeric_limits<double>::infinity(),
      true,
      true,
      -1.0,
      false,
      true,
      false));

  ranges.push_back(std::make_unique<DoubleRange>(
      1.0,
      false,
      true,
      std::numeric_limits<double>::infinity(),
      true,
      true,
      false));

  DoubleMultiRange multiRange(std::move(ranges), true);

  // Test special values
  EXPECT_TRUE(multiRange.testDouble(-std::numeric_limits<double>::infinity()));
  EXPECT_TRUE(multiRange.testDouble(-2.0));
  EXPECT_FALSE(multiRange.testDouble(-1.0));
  EXPECT_FALSE(multiRange.testDouble(0.0));
  EXPECT_FALSE(multiRange.testDouble(1.0));
  EXPECT_TRUE(multiRange.testDouble(2.0));
  EXPECT_TRUE(multiRange.testDouble(std::numeric_limits<double>::infinity()));
  EXPECT_FALSE(multiRange.testDouble(std::numeric_limits<double>::quiet_NaN()));
  EXPECT_TRUE(multiRange.testNull());
}

TEST_F(FloatingPointFiltersTest, MultiRangeBinarySearchAccuracy) {
  std::vector<std::unique_ptr<FloatingPointRange<double>>> ranges;

  // Create non-overlapping ranges
  ranges.push_back(std::make_unique<DoubleRange>(
      0.0,
      true,
      false, // [0.0     (inclusive start)
      1.0,
      false,
      true, // ,1.0)    (exclusive end)
      false));

  ranges.push_back(std::make_unique<DoubleRange>(
      1.0,
      false,
      true, // (1.0     (exclusive start)
      2.0,
      false,
      true, // ,2.0)    (exclusive end)
      false));

  ranges.push_back(std::make_unique<DoubleRange>(
      2.0,
      false,
      true, // (2.0     (exclusive start)
      3.0,
      false,
      true, // ,3.0)    (exclusive end)
      false));

  DoubleMultiRange multiRange(std::move(ranges), false);

  // Test boundary conditions
  EXPECT_TRUE(multiRange.testDouble(0.0)); // First range inclusive start
  EXPECT_TRUE(multiRange.testDouble(0.5)); // Inside first range
  EXPECT_FALSE(multiRange.testDouble(
      1.0)); // Between first and second (excluded by both)
  EXPECT_TRUE(multiRange.testDouble(1.5)); // Inside second range
  EXPECT_FALSE(multiRange.testDouble(
      2.0)); // Between second and third (excluded by both)
  EXPECT_TRUE(multiRange.testDouble(2.5)); // Inside third range
  EXPECT_FALSE(multiRange.testDouble(3.0)); // Last range exclusive end
}

TEST_F(FloatingPointFiltersTest, SerializationEdgeCases) {
  // Test serialization with special values
  std::vector<double> values = {
      std::numeric_limits<double>::min(),
      std::numeric_limits<double>::max(),
      std::numeric_limits<double>::lowest(),
      0.0,
      -0.0};

  DoubleValues original(values, true);
  auto serialized = original.serialize();
  auto deserialized = DoubleValues::create(serialized);

  EXPECT_TRUE(original.testingEquals(*deserialized));

  // Verify special values are preserved
  EXPECT_TRUE(deserialized->testDouble(std::numeric_limits<double>::min()));
  EXPECT_TRUE(deserialized->testDouble(std::numeric_limits<double>::max()));
  EXPECT_TRUE(deserialized->testDouble(std::numeric_limits<double>::lowest()));
  EXPECT_TRUE(deserialized->testDouble(0.0));
  EXPECT_TRUE(deserialized->testDouble(-0.0));
}

TEST_F(FloatingPointFiltersTest, NegatedRangeInteractions) {
  // Test interactions between different types of negated ranges
  auto negatedRange = std::make_unique<NegatedDoubleRange>(
      1.0, false, false, 3.0, false, false, true);

  std::vector<double> values = {0.0, 2.0, 4.0};
  auto valueFilter = std::make_unique<DoubleValues>(values, false);

  auto merged = negatedRange->mergeWith(valueFilter.get());
  ASSERT_EQ(merged, nullptr);
}

TEST_F(FloatingPointFiltersTest, MultipleNonOverlappingRanges) {
  std::vector<std::unique_ptr<FloatingPointRange<float>>> ranges;

  // Create non-overlapping ranges with gaps between them
  ranges.push_back(std::make_unique<FloatRange>(
      1.0f,
      false,
      true, // (1.0
      2.0f,
      false,
      true, // ,2.0)
      false));

  ranges.push_back(std::make_unique<FloatRange>(
      3.0f,
      false,
      true, // (3.0
      4.0f,
      false,
      true, // ,4.0)
      false));

  ranges.push_back(std::make_unique<FloatRange>(
      6.0f,
      false,
      true, // (6.0
      7.0f,
      false,
      true, // ,7.0)
      false));

  FloatMultiRange multiRange(std::move(ranges), false);

  // Test points
  EXPECT_FALSE(multiRange.testFloat(0.5f)); // Before first range
  EXPECT_TRUE(multiRange.testFloat(1.5f)); // In first range
  EXPECT_FALSE(multiRange.testFloat(2.5f)); // Between first and second range
  EXPECT_TRUE(multiRange.testFloat(3.5f)); // In second range
  EXPECT_FALSE(multiRange.testFloat(4.5f)); // Between second and third range
  EXPECT_TRUE(multiRange.testFloat(6.5f)); // In third range
  EXPECT_FALSE(multiRange.testFloat(7.5f)); // After last range
}

TEST_F(FloatingPointFiltersTest, NonOverlappingRanges) {
  std::vector<std::unique_ptr<FloatingPointRange<float>>> ranges;

  // Create strictly ordered, non-overlapping ranges
  ranges.push_back(std::make_unique<FloatRange>(
      -3.0f,
      false,
      true, // (-3.0
      -1.0f,
      false,
      true, // ,-1.0)
      false));

  ranges.push_back(std::make_unique<FloatRange>(
      0.0f,
      false,
      true, // (0.0
      2.0f,
      false,
      true, // ,2.0)
      false));

  ranges.push_back(std::make_unique<FloatRange>(
      3.0f,
      false,
      true, // (3.0
      4.0f,
      false,
      true, // ,4.0)
      false));

  FloatMultiRange multiRange(std::move(ranges), true);

  // Test points in various ranges
  EXPECT_FALSE(multiRange.testFloat(-3.5f)); // Before first range
  EXPECT_TRUE(multiRange.testFloat(-2.0f)); // In first range
  EXPECT_FALSE(multiRange.testFloat(-0.5f)); // Between first and second range
  EXPECT_TRUE(multiRange.testFloat(1.0f)); // In second range
  EXPECT_FALSE(multiRange.testFloat(2.5f)); // Between second and third range
  EXPECT_TRUE(multiRange.testFloat(3.5f)); // In third range
  EXPECT_FALSE(multiRange.testFloat(4.5f)); // After last range
}

TEST_F(FloatingPointFiltersTest, FloatingPointValuesNaNHandling) {
  std::vector<double> values = {1.0, 2.0};

  // Test with NaN allowed
  {
    DoubleValues filter_nan_allowed(values, false, true);
    EXPECT_TRUE(filter_nan_allowed.testDouble(
        std::numeric_limits<double>::quiet_NaN()));
    EXPECT_TRUE(filter_nan_allowed.testDouble(
        std::numeric_limits<double>::signaling_NaN()));
  }

  // Test with NaN not allowed
  {
    DoubleValues filter_nan_not_allowed(values, false, false);
    EXPECT_FALSE(filter_nan_not_allowed.testDouble(
        std::numeric_limits<double>::quiet_NaN()));
    EXPECT_FALSE(filter_nan_not_allowed.testDouble(
        std::numeric_limits<double>::signaling_NaN()));
  }
}

TEST_F(FloatingPointFiltersTest, BasicRangeChecks) {
  // Suppose our filter passes exactly these three values: {1.0, 2.0, 3.0}.
  std::vector<double> vals = {1.0, 2.0, 3.0};

  // nullAllowed = false, nanAllowed = false
  FloatingPointValues<double> filter(
      vals, /* nullAllowed = */ false, /* nanAllowed = */ false);

  // 1) Range that definitely contains at least one of {1.0,2.0,3.0}
  //    e.g. [0.5, 1.5] contains 1.0 => testDoubleRange should return true
  EXPECT_TRUE(filter.testDoubleRange(0.5, 1.5, /* hasNull = */ false));

  // 2) Range [1.0, 1.0] (exact single value) => contains 1.0 => should return
  // true
  EXPECT_TRUE(filter.testDoubleRange(1.0, 1.0, false));

  // 3) Range [1.1, 1.9] => does not contain 1.0 or 2.0 => should return false
  EXPECT_FALSE(filter.testDoubleRange(1.1, 1.9, false));

  // 4) Range [2.5, 2.5] => single point at 2.5 => not in {1.0,2.0,3.0} => false
  EXPECT_FALSE(filter.testDoubleRange(2.5, 2.5, false));

  // 5) Range that covers [2.0, 2.0] => single point at 2.0 => in the set =>
  // true
  EXPECT_TRUE(filter.testDoubleRange(2.0, 2.0, false));

  // 6) Range [3.0, 3.0] => single point at 3.0 => in the set => true
  EXPECT_TRUE(filter.testDoubleRange(3.0, 3.0, false));

  // 7) A large range [0.0, 5.0] => definitely includes 1.0,2.0,3.0 => true
  EXPECT_TRUE(filter.testDoubleRange(0.0, 5.0, false));

  // 8) Range [1.0, 1.001], should still capture 1.0 => true
  EXPECT_TRUE(filter.testDoubleRange(1.0, 1.001, false));

  // 9) Reverse range: min > max => no values
  EXPECT_FALSE(filter.testDoubleRange(5.0, 1.0, false));
}

TEST_F(FloatingPointFiltersTest, NullAllowedChecks) {
  // Filter still has {1.0, 2.0, 3.0}, but now nullAllowed = true
  std::vector<double> vals = {1.0, 2.0, 3.0};
  FloatingPointValues<double> filter(
      vals, /* nullAllowed = */ true, /* nanAllowed = */ false);

  // If we have a chunk with nulls (hasNull = true), we return true immediately
  EXPECT_TRUE(filter.testDoubleRange(100.0, 200.0, /* hasNull = */ true));

  // Even if the numeric range [100.0, 200.0] doesn't overlap with
  // {1.0,2.0,3.0}, the presence of nulls + nullAllowed => return true
  EXPECT_TRUE(filter.testDoubleRange(100.0, 200.0, /* hasNull = */ true));

  // If hasNull = false, then normal numeric logic applies:
  EXPECT_FALSE(filter.testDoubleRange(100.0, 200.0, /* hasNull = */ false));
}

TEST_F(FloatingPointFiltersTest, NanAllowedChecks) {
  // Suppose we have set = {1.0, 1.5}, nullAllowed = false, but nanAllowed =
  // true
  std::vector<double> vals = {1.0, 1.5};
  FloatingPointValues<double> filter(
      vals, /* nullAllowed = */ false, /* nanAllowed = */ true);

  // Check a normal numeric range that doesn't include 1.0 => false
  EXPECT_FALSE(filter.testDoubleRange(2.0, 3.0, false));

  // If the min or max is NaN => testDoubleRange returns nanAllowed_ (which is
  // true)
  EXPECT_TRUE(filter.testDoubleRange(
      std::numeric_limits<double>::quiet_NaN(), 3.0, false));
  EXPECT_TRUE(filter.testDoubleRange(
      1.0, std::numeric_limits<double>::quiet_NaN(), false));

  // If both bounds are NaN => still returns nanAllowed_ => true
  EXPECT_TRUE(filter.testDoubleRange(
      std::numeric_limits<double>::quiet_NaN(),
      std::numeric_limits<double>::quiet_NaN(),
      false));

  // Check with hasNull = true (though nullAllowed_ is false), it won't matter
  // if any bound is NaN => returns true because nanAllowed_ is true
  EXPECT_TRUE(filter.testDoubleRange(
      std::numeric_limits<double>::quiet_NaN(),
      std::numeric_limits<double>::infinity(),
      true));
}

TEST_F(FloatingPointFiltersTest, NanNotAllowedChecks) {
  // Suppose we have set = {2.0, 2.001}, nullAllowed = false, nanAllowed = false
  std::vector<double> vals = {2.0, 2.001};
  FloatingPointValues<double> filter(
      vals, /* nullAllowed = */ false, /* nanAllowed = */ false);

  // If either bound is NaN => testDoubleRange returns nanAllowed_ => false
  EXPECT_FALSE(filter.testDoubleRange(
      std::numeric_limits<double>::quiet_NaN(), 10.0, false));
  EXPECT_FALSE(filter.testDoubleRange(
      0.0, std::numeric_limits<double>::quiet_NaN(), false));
  EXPECT_FALSE(filter.testDoubleRange(
      std::numeric_limits<double>::quiet_NaN(),
      std::numeric_limits<double>::quiet_NaN(),
      false));

  // Check a normal numeric range that includes 2.0 => true
  EXPECT_TRUE(filter.testDoubleRange(1.0, 3.0, false));

  // Check a normal numeric range that excludes 2.0 => false
  EXPECT_FALSE(filter.testDoubleRange(2.01, 3.0, false));
  EXPECT_FALSE(filter.testDoubleRange(-100.0, 1.99, false));
}

TEST_F(FloatingPointFiltersTest, TestDoubleRange) {
  // Basic test cases with doubles
  {
    SCOPED_TRACE("Basic double value tests");
    std::vector<double> values = {1.0, 2.5, 4.0, 7.5, 10.0};
    FloatingPointValues<double> filter(values, /*nullAllowed=*/false);

    // Exact matches for range bounds
    SCOPED_TRACE("Exact range bound matches");
    EXPECT_TRUE(filter.testDoubleRange(1.0, 10.0, false));
    EXPECT_TRUE(filter.testDoubleRange(2.5, 7.5, false));
    EXPECT_TRUE(filter.testDoubleRange(4.0, 4.0, false));

    // Ranges that don't exactly match values but contain values
    SCOPED_TRACE("Ranges containing but not exactly matching values");
    EXPECT_TRUE(filter.testDoubleRange(0.5, 3.0, false));
    EXPECT_TRUE(filter.testDoubleRange(2.0, 5.0, false));
    EXPECT_TRUE(filter.testDoubleRange(7.0, 11.0, false));

    // Ranges that miss all values
    SCOPED_TRACE("Ranges missing all values");
    EXPECT_FALSE(filter.testDoubleRange(0.0, 0.9, false));
    EXPECT_FALSE(filter.testDoubleRange(1.1, 2.4, false));
    EXPECT_FALSE(filter.testDoubleRange(10.1, 20.0, false));
    EXPECT_FALSE(filter.testDoubleRange(2.6, 3.9, false));
  }

  // Test with float values
  {
    SCOPED_TRACE("Float value precision tests");
    std::vector<float> values = {1.0f, 2.5f, 4.0f, 7.5f, 10.0f};
    FloatingPointValues<float> filter(values, /*nullAllowed=*/false);

    SCOPED_TRACE("Float-double precision boundary tests");
    EXPECT_TRUE(filter.testDoubleRange(1.0, 1.0, false));
    EXPECT_TRUE(filter.testDoubleRange(1.0 - 1e-7, 1.0 + 1e-7, false));
    EXPECT_TRUE(filter.testDoubleRange(2.5 - 1e-7, 2.5 + 1e-7, false));
  }

  // Test null handling
  {
    SCOPED_TRACE("Null handling tests");
    std::vector<double> values = {1.0, 2.0, 3.0};
    FloatingPointValues<double> filterWithNull(values, /*nullAllowed=*/true);
    FloatingPointValues<double> filterNoNull(values, /*nullAllowed=*/false);

    SCOPED_TRACE("Tests with nulls allowed");
    EXPECT_TRUE(filterWithNull.testDoubleRange(0.0, 4.0, true));
    EXPECT_TRUE(filterWithNull.testDoubleRange(
        5.0, 6.0, true)); // No values in range but null allowed

    SCOPED_TRACE("Tests with nulls not allowed");
    EXPECT_TRUE(filterNoNull.testDoubleRange(0.0, 4.0, true));
    EXPECT_FALSE(filterNoNull.testDoubleRange(5.0, 6.0, true));
  }

  // Test NaN handling
  {
    SCOPED_TRACE("NaN handling tests");
    std::vector<double> values = {1.0, 2.0, 3.0};
    FloatingPointValues<double> filterWithNaN(
        values, /*nullAllowed=*/false, /*nanAllowed=*/true);
    FloatingPointValues<double> filterNoNaN(
        values, /*nullAllowed=*/false, /*nanAllowed=*/false);

    SCOPED_TRACE("Tests with NaN allowed");
    EXPECT_TRUE(filterWithNaN.testDoubleRange(std::nan(""), 2.0, false));
    EXPECT_TRUE(filterWithNaN.testDoubleRange(1.0, std::nan(""), false));

    SCOPED_TRACE("Tests with NaN not allowed");
    EXPECT_FALSE(filterNoNaN.testDoubleRange(std::nan(""), 2.0, false));
    EXPECT_FALSE(filterNoNaN.testDoubleRange(1.0, std::nan(""), false));
  }

  // Test invalid ranges
  {
    SCOPED_TRACE("Invalid range tests");
    std::vector<double> values = {1.0, 2.0, 3.0};
    FloatingPointValues<double> filter(values, /*nullAllowed=*/false);

    SCOPED_TRACE("Tests with min > max");
    EXPECT_FALSE(filter.testDoubleRange(2.0, 1.0, false));
    EXPECT_FALSE(filter.testDoubleRange(3.0, -1.0, false));
  }

  // Test with infinite values
  {
    SCOPED_TRACE("Infinite value tests");
    std::vector<double> values = {1.0, 2.0, 3.0};
    FloatingPointValues<double> filter(values, /*nullAllowed=*/false);

    SCOPED_TRACE("Tests with infinite bounds");
    EXPECT_TRUE(filter.testDoubleRange(
        -std::numeric_limits<double>::infinity(),
        std::numeric_limits<double>::infinity(),
        false));
    EXPECT_TRUE(filter.testDoubleRange(
        -std::numeric_limits<double>::infinity(), 2.0, false));
    EXPECT_TRUE(filter.testDoubleRange(
        2.0, std::numeric_limits<double>::infinity(), false));
    EXPECT_FALSE(filter.testDoubleRange(
        -std::numeric_limits<double>::infinity(), 0.5, false));
    EXPECT_FALSE(filter.testDoubleRange(
        3.5, std::numeric_limits<double>::infinity(), false));
  }

  // Test with very close values
  {
    SCOPED_TRACE("Tests with very close floating point values");
    // Use larger spacing to avoid precision issues
    std::vector<double> values = {1.0, 1.0 + 1e-12, 1.0 + 2e-12};
    FloatingPointValues<double> filter(values, /*nullAllowed=*/false);

    SCOPED_TRACE("Precision boundary tests");
    EXPECT_TRUE(filter.testDoubleRange(1.0, 1.0 + 1e-12, false));
    EXPECT_TRUE(filter.testDoubleRange(1.0 + 0.9e-12, 1.0 + 1.1e-12, false));
    // Use wider gap to ensure values are distinctly outside range
    EXPECT_FALSE(filter.testDoubleRange(1.0 + 2.5e-12, 1.0 + 3e-12, false));
  }

  // Add new test case for more realistic precision scenarios
  {
    SCOPED_TRACE("Realistic precision boundary tests");
    std::vector<double> values = {1.0, 1.001, 1.002};
    FloatingPointValues<double> filter(values, /*nullAllowed=*/false);

    EXPECT_TRUE(filter.testDoubleRange(1.0, 1.001, false));
    EXPECT_TRUE(filter.testDoubleRange(1.0005, 1.0015, false));
    EXPECT_FALSE(filter.testDoubleRange(1.0021, 1.0029, false));
  }
}