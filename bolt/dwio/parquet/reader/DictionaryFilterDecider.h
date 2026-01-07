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

#include <cstddef>
#include <functional>
#include <memory>
#include <vector>
namespace bytedance::bolt::parquet {

// Forward declarations
class DictionaryFilterContext;

// Abstract interface for decision making
class IDictionaryFilterDecider {
 public:
  virtual ~IDictionaryFilterDecider() = default;
  virtual bool shouldApplyFiltering() const = 0;
};

// Factory type definition
using DictionaryFilterDeciderFactory =
    std::function<std::unique_ptr<IDictionaryFilterDecider>(
        const DictionaryFilterContext&)>;

// Abstract base class for filter strategies
class FilterStrategy {
 public:
  enum class Type {
    SmallDictionary,
    LargeDictionary,
    FilterBreadth,
    Selectivity,
    RowGroupSize,
    Uniqueness,
    Custom
  };

  virtual ~FilterStrategy() = default;
  virtual double evaluate(const DictionaryFilterContext& context) const = 0;
  virtual Type type() const = 0;
};

// Context class containing all the data needed for filtering decisions
class DictionaryFilterContext {
 public:
  DictionaryFilterContext(
      size_t dictElements,
      size_t filterElements,
      size_t rowGroupRows = 0,
      size_t maxDictSizeThreshold = 10000,
      size_t maxFilterSizeThreshold = 1000,
      double minBenefitRatio = 0.2,
      size_t smallDictionaryThreshold = 10,
      double maxUniqueRatioThreshold = 0.8,
      size_t rowGroupMinRows = 1000);

  // Getters
  size_t dictElements() const {
    return dictElements_;
  }
  size_t filterElements() const {
    return filterElements_;
  }
  size_t rowGroupRows() const {
    return rowGroupRows_;
  }
  size_t maxDictSizeThreshold() const {
    return maxDictSizeThreshold_;
  }
  size_t maxFilterSizeThreshold() const {
    return maxFilterSizeThreshold_;
  }
  double minBenefitRatio() const {
    return minBenefitRatio_;
  }
  size_t smallDictionaryThreshold() const {
    return smallDictionaryThreshold_;
  }
  double maxUniqueRatioThreshold() const {
    return maxUniqueRatioThreshold_;
  }
  size_t rowGroupMinRows() const {
    return rowGroupMinRows_;
  }

 private:
  size_t dictElements_;
  size_t filterElements_;
  size_t rowGroupRows_;
  size_t maxDictSizeThreshold_;
  size_t maxFilterSizeThreshold_;
  double minBenefitRatio_;
  size_t smallDictionaryThreshold_;
  double maxUniqueRatioThreshold_;
  size_t rowGroupMinRows_;
};

// Concrete strategy implementations
class SmallDictionaryStrategy : public FilterStrategy {
 public:
  static const SmallDictionaryStrategy& instance() {
    static SmallDictionaryStrategy instance;
    return instance;
  }

  double evaluate(const DictionaryFilterContext& context) const override;
  Type type() const override {
    return Type::SmallDictionary;
  }

 private:
  SmallDictionaryStrategy() = default;
};

class LargeDictionaryStrategy : public FilterStrategy {
 public:
  static const LargeDictionaryStrategy& instance() {
    static LargeDictionaryStrategy instance;
    return instance;
  }

  double evaluate(const DictionaryFilterContext& context) const override;
  Type type() const override {
    return Type::LargeDictionary;
  }

 private:
  LargeDictionaryStrategy() = default;
};

class FilterBreadthStrategy : public FilterStrategy {
 public:
  static const FilterBreadthStrategy& instance() {
    static FilterBreadthStrategy instance;
    return instance;
  }

  double evaluate(const DictionaryFilterContext& context) const override;
  Type type() const override {
    return Type::FilterBreadth;
  }

 private:
  FilterBreadthStrategy() = default;
};

class SelectivityStrategy : public FilterStrategy {
 public:
  static const SelectivityStrategy& instance() {
    static SelectivityStrategy instance;
    return instance;
  }

  double evaluate(const DictionaryFilterContext& context) const override;
  Type type() const override {
    return Type::Selectivity;
  }

 private:
  SelectivityStrategy() = default;
};

class RowGroupSizeStrategy : public FilterStrategy {
 public:
  static const RowGroupSizeStrategy& instance() {
    static RowGroupSizeStrategy instance;
    return instance;
  }

  double evaluate(const DictionaryFilterContext& context) const override;
  Type type() const override {
    return Type::RowGroupSize;
  }

 private:
  RowGroupSizeStrategy() = default;
};

class UniquenessStrategy : public FilterStrategy {
 public:
  static const UniquenessStrategy& instance() {
    static UniquenessStrategy instance;
    return instance;
  }

  double evaluate(const DictionaryFilterContext& context) const override;
  Type type() const override {
    return Type::Uniqueness;
  }

 private:
  UniquenessStrategy() = default;
};

// Main decision maker class
class WeightedDictionaryFilterDecider : public IDictionaryFilterDecider {
 public:
  explicit WeightedDictionaryFilterDecider(
      const DictionaryFilterContext& context);

  // Allow custom strategy injection
  void addStrategy(const FilterStrategy* strategy);
  bool shouldApplyFiltering() const override;

  // For debugging and monitoring
  std::vector<std::pair<FilterStrategy::Type, double>> getStrategyScores()
      const;

 private:
  DictionaryFilterContext context_;
  std::vector<const FilterStrategy*> strategies_;
};

// Factory for creating deciders
class DeciderFactory {
 public:
  static void setFactory(DictionaryFilterDeciderFactory factory) {
    customFactory_ = std::move(factory);
  }

  static void resetFactory() {
    customFactory_ = nullptr;
  }

  static std::unique_ptr<IDictionaryFilterDecider> create(
      const DictionaryFilterContext& context) {
    if (customFactory_) {
      return customFactory_(context);
    }
    return std::make_unique<WeightedDictionaryFilterDecider>(context);
  }

 private:
  static DictionaryFilterDeciderFactory customFactory_;
};

// Simple override decider for testing
class ConstantDictionaryFilterDecider : public IDictionaryFilterDecider {
 public:
  explicit ConstantDictionaryFilterDecider(bool shouldFilter)
      : shouldFilter_(shouldFilter) {}

  bool shouldApplyFiltering() const override {
    return shouldFilter_;
  }

 private:
  bool shouldFilter_;
};
} // namespace bytedance::bolt::parquet