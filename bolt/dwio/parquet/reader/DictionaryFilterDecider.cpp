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

#include "bolt/dwio/parquet/reader/DictionaryFilterDecider.h"
#include <common/base/Exceptions.h>
#include <common/memory/MemoryPool.h>
#include <glog/logging.h>
#include <glog/vlog_is_on.h>
namespace bytedance::bolt::parquet {

// Initialize static member
DictionaryFilterDeciderFactory DeciderFactory::customFactory_;

DictionaryFilterContext::DictionaryFilterContext(
    size_t dictElements,
    size_t filterElements,
    size_t rowGroupRows,
    size_t maxDictSizeThreshold,
    size_t maxFilterSizeThreshold,
    double minBenefitRatio,
    size_t smallDictionaryThreshold,
    double maxUniqueRatioThreshold,
    size_t rowGroupMinRows)
    : dictElements_(dictElements),
      filterElements_(filterElements),
      rowGroupRows_(rowGroupRows),
      maxDictSizeThreshold_(maxDictSizeThreshold),
      maxFilterSizeThreshold_(maxFilterSizeThreshold),
      minBenefitRatio_(minBenefitRatio),
      smallDictionaryThreshold_(smallDictionaryThreshold),
      maxUniqueRatioThreshold_(maxUniqueRatioThreshold),
      rowGroupMinRows_(rowGroupMinRows) {}

double SmallDictionaryStrategy::evaluate(
    const DictionaryFilterContext& context) const {
  bool isSmall = context.dictElements() <= context.smallDictionaryThreshold();
  double score = isSmall ? 2.0 : 0.0;
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "SmallDictionaryStrategy: dictElements="
            << context.dictElements()
            << " threshold=" << context.smallDictionaryThreshold()
            << " score=" << score;
  }
  return score;
}

double LargeDictionaryStrategy::evaluate(
    const DictionaryFilterContext& context) const {
  bool isLarge = context.dictElements() > context.maxDictSizeThreshold();
  double score = isLarge ? -1.0 : 0.0;
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "LargeDictionaryStrategy: dictElements="
            << context.dictElements()
            << " threshold=" << context.maxDictSizeThreshold()
            << " score=" << score;
  }
  return score;
}

double FilterBreadthStrategy::evaluate(
    const DictionaryFilterContext& context) const {
  bool isBroad = context.filterElements() > context.maxFilterSizeThreshold();
  double score = isBroad ? -1.0 : 0.0;
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "FilterBreadthStrategy: filterElements="
            << context.filterElements()
            << " threshold=" << context.maxFilterSizeThreshold()
            << " score=" << score;
  }
  return score;
}

double SelectivityStrategy::evaluate(
    const DictionaryFilterContext& context) const {
  double selectivityRatio =
      static_cast<double>(context.filterElements()) / context.dictElements();
  double score = selectivityRatio > context.minBenefitRatio() ? -1.0 : 1.0;
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "SelectivityStrategy: ratio=" << selectivityRatio
            << " threshold=" << context.minBenefitRatio() << " score=" << score;
  }
  return score;
}

double RowGroupSizeStrategy::evaluate(
    const DictionaryFilterContext& context) const {
  if (context.rowGroupRows() == 0) {
    VLOG(2) << "RowGroupSizeStrategy: rowGroupRows=0, score=0.0";
    return 0.0;
  }
  bool isTooSmall = context.rowGroupRows() < context.rowGroupMinRows();
  double score = isTooSmall ? -1.0 : 1.0;
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "RowGroupSizeStrategy: rowGroupRows=" << context.rowGroupRows()
            << " threshold=" << context.rowGroupMinRows() << " score=" << score;
  }
  return score;
}

double UniquenessStrategy::evaluate(
    const DictionaryFilterContext& context) const {
  if (context.rowGroupRows() == 0 || context.dictElements() == 0) {
    if (VLOG_IS_ON(2)) {
      VLOG(2) << fmt::format(
          "UniquenessStrategy: rowGroupRows={}, dictElement={}, score=0.0",
          context.rowGroupRows(),
          context.dictElements());
    }
    return 0.0;
  }

  // Leveled score based on the ratio between unique ratio and maxUniqueRatio.
  double uniqueRatio =
      static_cast<double>(context.dictElements()) / context.rowGroupRows();
  double score = context.maxUniqueRatioThreshold() / uniqueRatio;
  if (score < 1) {
    score = -1;
  } else {
    score = std::min(score, 2.0);
  }

  if (VLOG_IS_ON(2)) {
    VLOG(2) << "UniquenessStrategy: ratio=" << uniqueRatio
            << " threshold=" << context.maxUniqueRatioThreshold()
            << " score=" << score;
  }
  return score;
}

WeightedDictionaryFilterDecider::WeightedDictionaryFilterDecider(
    const DictionaryFilterContext& context)
    : context_(context) {
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "Creating WeightedDictionaryFilterDecider with:"
            << "\n  dictElements: " << context.dictElements()
            << "\n  filterElements: " << context.filterElements()
            << "\n  rowGroupRows: " << context.rowGroupRows();
  }
  // Initialize default strategies with singleton instances
  strategies_.push_back(&SmallDictionaryStrategy::instance());
  strategies_.push_back(&LargeDictionaryStrategy::instance());
  strategies_.push_back(&FilterBreadthStrategy::instance());
  strategies_.push_back(&SelectivityStrategy::instance());
  strategies_.push_back(&RowGroupSizeStrategy::instance());
  strategies_.push_back(&UniquenessStrategy::instance());
}

void WeightedDictionaryFilterDecider::addStrategy(
    const FilterStrategy* strategy) {
  BOLT_CHECK(strategy, "Cannot add null strategy");
  strategies_.push_back(strategy);
}

bool WeightedDictionaryFilterDecider::shouldApplyFiltering() const {
  // Early exit if no dictionary
  if (context_.dictElements() == 0) {
    VLOG(2) << "No dictionary elements present, skipping filtering";
    return false;
  }

  double totalScore = 0.0;
  for (const auto& strategy : strategies_) {
    double score = strategy->evaluate(context_);
    totalScore += score;
  }

  bool shouldFilter = totalScore > 0.0;
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "Final decision: " << (shouldFilter ? "APPLY" : "SKIP")
            << " filtering (total score: " << totalScore << ")";
  }
  return shouldFilter;
}

std::vector<std::pair<FilterStrategy::Type, double>>
WeightedDictionaryFilterDecider::getStrategyScores() const {
  std::vector<std::pair<FilterStrategy::Type, double>> scores;
  for (const auto& strategy : strategies_) {
    scores.emplace_back(strategy->type(), strategy->evaluate(context_));
  }
  return scores;
}
} // namespace bytedance::bolt::parquet
