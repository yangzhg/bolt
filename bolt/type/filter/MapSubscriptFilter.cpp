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

#include "bolt/type/filter/MapSubscriptFilter.h"
#include <memory>
#include "bolt/common/serialization/Serializable.h"
namespace bytedance::bolt::common {

MapSubscriptFilter::MapSubscriptFilter(
    const std::string& key,
    std::shared_ptr<const Filter> valueFilter,
    std::shared_ptr<const Filter> keyFilter)
    : Filter(
          true,
          valueFilter ? valueFilter->testNull() : true,
          FilterKind::kMapSubscript),
      keyType_(KeyType::STRING),
      stringKey_(key),
      intKey_(0),
      valueFilter_(std::move(valueFilter)),
      keyFilter_(std::move(keyFilter)) {}

MapSubscriptFilter::MapSubscriptFilter(
    int64_t key,
    std::shared_ptr<const Filter> valueFilter,
    std::shared_ptr<const Filter> keyFilter)
    : Filter(
          true,
          valueFilter ? valueFilter->testNull() : true,
          FilterKind::kMapSubscript),
      keyType_(KeyType::INTEGER),
      stringKey_(""),
      intKey_(key),
      valueFilter_(std::move(valueFilter)),
      keyFilter_(std::move(keyFilter)) {}

std::unique_ptr<Filter> MapSubscriptFilter::clone(
    std::optional<bool> nullAllowed) const {
  if (valueFilter_) {
    auto newValueFilter = nullAllowed.has_value()
        ? valueFilter_->clone(nullAllowed.value())
        : valueFilter_->clone();
    if (keyType_ == KeyType::STRING) {
      return createMapSubscriptFilter(stringKey_, std::move(newValueFilter));
    } else {
      return createMapSubscriptFilter(intKey_, std::move(newValueFilter));
    }
  }
  if (keyType_ == KeyType::STRING) {
    return createMapSubscriptFilter(stringKey_);
  } else {
    return createMapSubscriptFilter(intKey_);
  }
}

std::string MapSubscriptFilter::toString() const {
  if (keyType_ == KeyType::STRING) {
    if (valueFilter_) {
      return fmt::format(
          "MAP_SUBSCRIPT(key={}, valueFilter={})",
          stringKey_,
          valueFilter_->toString());
    }
    return fmt::format("MAP_SUBSCRIPT(key={})", stringKey_);
  } else {
    if (valueFilter_) {
      return fmt::format(
          "MAP_SUBSCRIPT(key={}, valueFilter={})",
          intKey_,
          valueFilter_->toString());
    }
    return fmt::format("MAP_SUBSCRIPT(key={})", intKey_);
  }
}

folly::dynamic MapSubscriptFilter::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "MapSubscript";
  obj["keyType"] = keyType_ == KeyType::STRING ? "string" : "integer";
  if (keyType_ == KeyType::STRING) {
    obj["key"] = stringKey_;
  } else {
    obj["key"] = intKey_;
  }
  if (valueFilter_) {
    obj["valueFilter"] = valueFilter_->serialize();
  }
  obj["nullAllowed"] = this->testNull();
  return obj;
}

std::unique_ptr<Filter> MapSubscriptFilter::create(const folly::dynamic& obj) {
  std::shared_ptr<const Filter> valueFilter;
  if (obj.count("valueFilter")) {
    valueFilter = ISerializable::deserialize<Filter>(obj["valueFilter"]);
  }

  // Check if keyType is specified
  if (obj.count("keyType") && obj["keyType"].asString() == "integer") {
    auto key = obj["key"].asInt();
    return createMapSubscriptFilter(key, std::move(valueFilter));
  } else {
    auto key = obj["key"].asString();
    return createMapSubscriptFilter(key, std::move(valueFilter));
  }
}

bool MapSubscriptFilter::testingEquals(const Filter& other) const {
  if (this == &other) {
    return true;
  }
  if (!testingBaseEquals(other) || other.kind() != kind()) {
    return false;
  }
  const auto* otherMapSubscript =
      static_cast<const MapSubscriptFilter*>(&other);

  // Check if key types match
  if (keyType_ != otherMapSubscript->keyType_) {
    return false;
  }

  // Compare keys based on type
  if (keyType_ == KeyType::STRING) {
    if (stringKey_ != otherMapSubscript->stringKey_) {
      return false;
    }
  } else {
    if (intKey_ != otherMapSubscript->intKey_) {
      return false;
    }
  }

  if (valueFilter_ && otherMapSubscript->valueFilter_) {
    return valueFilter_->testingEquals(*otherMapSubscript->valueFilter_);
  }
  return valueFilter_ == otherMapSubscript->valueFilter_;
}

std::unique_ptr<Filter> createMapSubscriptFilter(
    const std::string& key,
    std::shared_ptr<const Filter> valueFilter,
    std::shared_ptr<const Filter> keyFilter) {
  return std::make_unique<MapSubscriptFilter>(
      key, std::move(valueFilter), std::move(keyFilter));
}

// Overload for integer keys
std::unique_ptr<Filter> createMapSubscriptFilter(
    int64_t key,
    std::shared_ptr<const Filter> valueFilter,
    std::shared_ptr<const Filter> keyFilter) {
  return std::make_unique<MapSubscriptFilter>(
      key, std::move(valueFilter), std::move(keyFilter));
}

std::unique_ptr<ISerializable> deserializeMapSubscriptFilter(
    const folly::dynamic& obj) {
  std::shared_ptr<const Filter> valueFilter;
  if (obj.count("valueFilter")) {
    valueFilter = ISerializable::deserialize<Filter>(obj["valueFilter"]);
  }

  // Check if keyType is specified
  if (obj.count("keyType") && obj["keyType"].asString() == "integer") {
    auto key = obj["key"].asInt();
    return createMapSubscriptFilter(key, std::move(valueFilter));
  } else {
    auto key = obj["key"].asString();
    return createMapSubscriptFilter(key, std::move(valueFilter));
  }
}

} // namespace bytedance::bolt::common