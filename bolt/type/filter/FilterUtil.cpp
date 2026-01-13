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

#include "bolt/type/filter/FilterUtil.h"
#include "bolt/type/filter/FilterBase.h"
#include "bolt/type/filter/FloatingPointMultiRange.h"
#include "bolt/type/filter/FloatingPointRange.h"
namespace bytedance::bolt::common {

std::unique_ptr<Filter> nullOrFalse(bool nullAllowed) {
  if (nullAllowed) {
    return std::make_unique<IsNull>();
  }
  return std::make_unique<AlwaysFalse>();
}

std::unique_ptr<Filter> notNullOrTrue(bool nullAllowed) {
  if (nullAllowed) {
    return std::make_unique<AlwaysTrue>();
  }
  return std::make_unique<IsNotNull>();
}
std::unique_ptr<Filter> combineNegatedRangeOnHugeintRanges(
    const int128_t& lower,
    const int128_t& upper,
    const std::vector<std::unique_ptr<HugeintRange>>& ranges,
    bool nullAllowed) {
  std::vector<std::unique_ptr<HugeintRange>> outRanges;

  // Handle each range
  for (const auto& range : ranges) {
    // If the range is completely outside the negated range, include it
    if (range->upper() < lower || range->lower() > upper) {
      outRanges.push_back(std::make_unique<HugeintRange>(
          range->lower(), range->upper(), false));
      continue;
    }

    // If the range extends below the negated range, add that part
    if (range->lower() < lower) {
      outRanges.push_back(
          std::make_unique<HugeintRange>(range->lower(), lower - 1, false));
    }

    // If the range extends above the negated range, add that part
    if (range->upper() > upper) {
      outRanges.push_back(
          std::make_unique<HugeintRange>(upper + 1, range->upper(), false));
    }
  }

  return combineHugeintRanges(std::move(outRanges), nullAllowed);
}

std::unique_ptr<Filter> combineNegatedRangeOnTimestampRanges(
    const Timestamp& lower,
    const Timestamp& upper,
    const std::vector<std::unique_ptr<TimestampRange>>& ranges,
    bool nullAllowed) {
  std::vector<std::unique_ptr<TimestampRange>> outRanges;

  // Handle each range
  for (const auto& range : ranges) {
    // If the range is completely outside the negated range, include it
    if (range->upper() < lower || range->lower() > upper) {
      outRanges.push_back(std::make_unique<TimestampRange>(
          range->lower(), range->upper(), false));
      continue;
    }

    // If the range extends below the negated range, add that part
    if (range->lower() < lower) {
      outRanges.push_back(
          std::make_unique<TimestampRange>(range->lower(), lower, false));
    }

    // If the range extends above the negated range, add that part
    if (range->upper() > upper) {
      outRanges.push_back(
          std::make_unique<TimestampRange>(upper, range->upper(), false));
    }
  }

  return combineTimestampRanges(std::move(outRanges), nullAllowed);
}

std::unique_ptr<Filter> combineTimestampRanges(
    std::vector<std::unique_ptr<TimestampRange>> ranges,
    bool nullAllowed) {
  if (ranges.empty()) {
    return nullOrFalse(nullAllowed);
  }

  // Sort ranges by lower bound
  std::sort(ranges.begin(), ranges.end(), [](const auto& a, const auto& b) {
    return a->lower() < b->lower();
  });

  // Merge overlapping or adjacent ranges
  std::vector<std::unique_ptr<TimestampRange>> mergedRanges;
  auto current = std::move(ranges[0]);

  for (size_t i = 1; i < ranges.size(); ++i) {
    if (ranges[i]->lower() <= current->upper()) {
      // Ranges overlap or are adjacent - merge them
      current = std::make_unique<TimestampRange>(
          current->lower(),
          std::max(current->upper(), ranges[i]->upper()),
          false);
    } else {
      // No overlap - add current range and start new one
      mergedRanges.push_back(std::move(current));
      current = std::move(ranges[i]);
    }
  }
  mergedRanges.push_back(std::move(current));

  if (mergedRanges.size() == 1) {
    return std::make_unique<TimestampRange>(
        mergedRanges.front()->lower(),
        mergedRanges.front()->upper(),
        nullAllowed);
  }

  // TODO: Implement TimestampMultiRange if needed
  auto first = mergedRanges.front().get();
  auto last = mergedRanges.back().get();
  return std::make_unique<TimestampRange>(
      first->lower(), last->upper(), nullAllowed);
}

std::unique_ptr<Filter> createBigintRange(
    int64_t min,
    int64_t max,
    bool nullAllowed,
    bool rangeAllowed) {
  if (min > max) {
    return nullOrFalse(nullAllowed);
  }
  return std::make_unique<BigintRange>(min, max, nullAllowed);
}

std::unique_ptr<Filter> createBytesValues(
    const std::vector<std::string>& values,
    bool nullAllowed) {
  // BytesValues constructor requires non-empty values
  if (values.empty()) {
    return nullOrFalse(nullAllowed);
  }

  return std::make_unique<BytesValues>(values, nullAllowed);
}

std::unique_ptr<Filter> createBytesValues(
    const std::vector<StringView>& values,
    bool nullAllowed) {
  if (values.empty()) {
    return nullOrFalse(nullAllowed);
  }

  return std::make_unique<BytesValues>(values, nullAllowed);
}

std::unique_ptr<Filter> createBytesRange(
    std::optional<std::string> lower,
    bool lowerInclusive,
    std::optional<std::string> upper,
    bool upperInclusive,
    bool nullAllowed) {
  return std::make_unique<BytesRange>(
      lower.value_or(""), // lower
      !lower.has_value(), // lowerUnbounded
      lower.has_value() && !lowerInclusive, // lowerExclusive
      upper.value_or(""), // upper
      !upper.has_value(), // upperUnbounded
      upper.has_value() && !upperInclusive, // upperExclusive
      nullAllowed);
}

std::unique_ptr<Filter> combineHugeintRanges(
    std::vector<std::unique_ptr<HugeintRange>> ranges,
    bool nullAllowed) {
  if (ranges.empty()) {
    return nullOrFalse(nullAllowed);
  }

  // Sort ranges by lower bound
  std::sort(ranges.begin(), ranges.end(), [](const auto& a, const auto& b) {
    return a->lower() < b->lower();
  });

  // Merge overlapping or adjacent ranges
  std::vector<std::unique_ptr<HugeintRange>> mergedRanges;
  auto current = std::move(ranges[0]);

  for (size_t i = 1; i < ranges.size(); ++i) {
    if (ranges[i]->lower() <= current->upper() + 1) {
      // Ranges overlap or are adjacent - merge them
      current = std::make_unique<HugeintRange>(
          current->lower(),
          std::max(current->upper(), ranges[i]->upper()),
          false);
    } else {
      // No overlap - add current range and start new one
      mergedRanges.push_back(std::move(current));
      current = std::move(ranges[i]);
    }
  }
  mergedRanges.push_back(std::move(current));

  if (mergedRanges.size() == 1) {
    return std::make_unique<HugeintRange>(
        mergedRanges.front()->lower(),
        mergedRanges.front()->upper(),
        nullAllowed);
  }

  // TODO: Implement HugeintMultiRange if needed
  auto first = mergedRanges.front().get();
  auto last = mergedRanges.back().get();
  return std::make_unique<HugeintRange>(
      first->lower(), last->upper(), nullAllowed);
}

std::unique_ptr<Filter> createNegatedBigintValues(
    const std::vector<int64_t>& values,
    bool nullAllowed) {
  return createBigintValuesFilter(values, nullAllowed, true);
}

std::unique_ptr<Filter> createBigintValues(
    const std::vector<int64_t>& values,
    bool nullAllowed) {
  return createBigintValuesFilter(values, nullAllowed, false);
}

std::unique_ptr<Filter> createBigintValuesFilter(
    const std::vector<int64_t>& values,
    bool nullAllowed,
    bool negated) {
  if (values.empty()) {
    if (!negated) {
      return nullOrFalse(nullAllowed);
    }
    return notNullOrTrue(nullAllowed);
  }
  if (values.size() == 1) {
    if (negated) {
      return std::make_unique<NegatedBigintRange>(
          values.front(), values.front(), nullAllowed);
    }
    return std::make_unique<BigintRange>(
        values.front(), values.front(), nullAllowed);
  }
  int64_t min = values[0];
  int64_t max = values[0];
  for (int i = 1; i < values.size(); ++i) {
    if (values[i] > max) {
      max = values[i];
    } else if (values[i] < min) {
      min = values[i];
    }
  }
  // If bitmap would have more than 4 words per set bit, we prefer a
  // hash table. If bitmap fits in under 32 words, we use bitmap anyhow.
  int64_t range;
  bool overflow = __builtin_sub_overflow(max, min, &range);
  if (LIKELY(!overflow)) {
    // all accepted/rejected values form one contiguous block
    if ((uint64_t)range + 1 == values.size()) {
      if (negated) {
        return std::make_unique<NegatedBigintRange>(min, max, nullAllowed);
      }
      return std::make_unique<BigintRange>(min, max, nullAllowed);
    }

    if (range < 32 * 64 || range < values.size() * 4 * 64) {
      if (negated) {
        return std::make_unique<NegatedBigintValuesUsingBitmask>(
            min, max, values, nullAllowed);
      }
      return std::make_unique<BigintValuesUsingBitmask>(
          min, max, values, nullAllowed);
    }
  }
  if (negated) {
    return std::make_unique<NegatedBigintValuesUsingHashTable>(
        min, max, values, nullAllowed);
  }
  return std::make_unique<BigintValuesUsingHashTable>(
      min, max, values, nullAllowed);
}

std::unique_ptr<Filter> createHugeintValues(
    const std::vector<int128_t>& values,
    bool nullAllowed) {
  int128_t min = *std::min_element(values.begin(), values.end());
  int128_t max = *std::max_element(values.begin(), values.end());

  return std::make_unique<HugeintValuesUsingHashTable>(
      min, max, values, nullAllowed);
}

template <typename T>
std::unique_ptr<Filter> combineFloatRanges(
    std::vector<std::unique_ptr<FloatingPointRange<T>>> ranges,
    bool nullAllowed) {
  if (ranges.empty()) {
    return std::make_unique<AlwaysFalse>();
  }

  // For a single range, just return it directly
  if (ranges.size() == 1) {
    auto& range = ranges.front();
    return std::make_unique<FloatingPointRange<T>>(
        range->lower(),
        range->lowerUnbounded(),
        range->lowerExclusive(),
        range->upper(),
        range->upperUnbounded(),
        range->upperExclusive(),
        nullAllowed);
  }

  // Sort ranges by lower bound
  std::sort(ranges.begin(), ranges.end(), [](const auto& a, const auto& b) {
    return a->lower() < b->lower();
  });

  // Merge overlapping ranges
  std::vector<std::unique_ptr<FloatingPointRange<T>>> mergedRanges;
  auto current = std::move(ranges[0]);

  for (size_t i = 1; i < ranges.size(); i++) {
    const auto& next = ranges[i];
    const T epsilon = std::numeric_limits<T>::epsilon();

    // If ranges overlap or are adjacent (within epsilon)
    if (current->upper() + epsilon >= next->lower()) {
      // Extend current range
      current = std::make_unique<FloatingPointRange<T>>(
          current->lower(),
          current->lowerUnbounded(),
          current->lowerExclusive(),
          std::max(current->upper(), next->upper()),
          current->upperUnbounded() || next->upperUnbounded(),
          current->upperExclusive() && next->upperExclusive(),
          false /*nullAllowed*/);
    } else {
      // Add current range and start new one
      mergedRanges.push_back(std::move(current));
      current = std::make_unique<FloatingPointRange<T>>(
          next->lower(),
          next->lowerUnbounded(),
          next->lowerExclusive(),
          next->upper(),
          next->upperUnbounded(),
          next->upperExclusive(),
          false /*nullAllowed*/);
    }
  }

  // Add final range
  if (current) {
    mergedRanges.push_back(std::move(current));
  }

  // If after merging we're down to one range, return it
  if (mergedRanges.size() == 1) {
    auto& range = mergedRanges.front();
    return std::make_unique<FloatingPointRange<T>>(
        range->lower(),
        range->lowerUnbounded(),
        range->lowerExclusive(),
        range->upper(),
        range->upperUnbounded(),
        range->upperExclusive(),
        nullAllowed);
  }

  // Return multi-range
  return std::make_unique<FloatingPointMultiRange<T>>(
      std::move(mergedRanges), nullAllowed);
}

template std::unique_ptr<Filter> combineFloatRanges<float>(
    std::vector<std::unique_ptr<FloatingPointRange<float>>> ranges,
    bool nullAllowed);

template std::unique_ptr<Filter> combineFloatRanges<double>(
    std::vector<std::unique_ptr<FloatingPointRange<double>>> ranges,
    bool nullAllowed);

} // namespace bytedance::bolt::common
