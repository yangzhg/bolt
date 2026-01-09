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
#include <optional>
#include <string>

#include "bolt/type/Type.h"
#include "bolt/type/filter/FilterBase.h"
namespace bytedance::bolt::common {

// A filter for map subscript operations that can filter on both key and value
class MapSubscriptFilter : public Filter {
 public:
  // Constructor for string keys
  MapSubscriptFilter(
      const std::string& key,
      std::shared_ptr<const Filter> valueFilter = nullptr,
      std::shared_ptr<const Filter> keyFilter = nullptr);

  // Constructor for integer keys
  MapSubscriptFilter(
      int64_t key,
      std::shared_ptr<const Filter> valueFilter = nullptr,
      std::shared_ptr<const Filter> keyFilter = nullptr);

  FilterKind kind() const {
    return FilterKind::kMapSubscript;
  }

  // Returns the string key if this filter uses a string key
  const std::string& stringKey() const {
    BOLT_CHECK(keyType_ == KeyType::STRING, "Not a string key");
    return stringKey_;
  }

  // Returns the integer key if this filter uses an integer key
  int64_t intKey() const {
    BOLT_CHECK(keyType_ == KeyType::INTEGER, "Not an integer key");
    return intKey_;
  }

  // Returns the key type
  bool isStringKey() const {
    return keyType_ == KeyType::STRING;
  }

  bool isIntKey() const {
    return keyType_ == KeyType::INTEGER;
  }

  const Filter* valueFilter() const {
    return valueFilter_.get();
  }

  const Filter* keyFilter() const {
    return keyFilter_.get();
  }

  bool isDeterministic() const override {
    return (keyFilter_ ? keyFilter_->isDeterministic() : true) &&
        (valueFilter_ ? valueFilter_->isDeterministic() : true);
  }

  std::unique_ptr<Filter> clone(
      std::optional<bool> nullAllowed = std::nullopt) const override;

  std::string toString() const override;

  bool testNull() const override {
    return valueFilter_ ? valueFilter_->testNull() : nullAllowed_;
  }

  folly::dynamic serialize() const override;

  static std::unique_ptr<Filter> create(const folly::dynamic& obj);

  bool testingEquals(const Filter& other) const override;

 private:
  enum class KeyType { STRING, INTEGER };

  const KeyType keyType_;
  const std::string stringKey_;
  const int64_t intKey_;
  const std::shared_ptr<const Filter> valueFilter_;
  const std::shared_ptr<const Filter> keyFilter_;
};

// Creates a MapSubscriptFilter with the specified string key and optional value
// filter.
std::unique_ptr<Filter> createMapSubscriptFilter(
    const std::string& key,
    std::shared_ptr<const Filter> valueFilter = nullptr,
    std::shared_ptr<const Filter> keyFilter = nullptr);

// Creates a MapSubscriptFilter with the specified integer key and optional
// value filter.
std::unique_ptr<Filter> createMapSubscriptFilter(
    int64_t key,
    std::shared_ptr<const Filter> valueFilter = nullptr,
    std::shared_ptr<const Filter> keyFilter = nullptr);

// Deserializes a MapSubscriptFilter from a dynamic object.
std::unique_ptr<ISerializable> deserializeMapSubscriptFilter(
    const folly::dynamic& obj);

} // namespace bytedance::bolt::common
