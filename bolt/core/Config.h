/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <folly/Conv.h>
#include <folly/Optional.h>
#include <folly/Synchronized.h>
#include <mutex>
#include <set>
#include <typeindex>
#include <unordered_map>
#include "bolt/common/base/Exceptions.h"
namespace bytedance::bolt {

class Config {
 public:
  virtual ~Config() = default;

  virtual folly::Optional<std::string> get(const std::string& key) const = 0;
  // virtual const string operator[](const std::string& key) = 0;
  // overload and disable not supported cases.

  template <typename T>
  folly::Optional<T> get(const std::string& key) const {
    auto val = get(key);
    if (val.hasValue()) {
      T ret;
      try {
        ret = folly::to<T>(val.value());
      } catch (folly::ConversionError const& e) {
        BOLT_FAIL("Exception in Config get : {}", e.what());
      }
      return ret;
    } else {
      return folly::none;
    }
  }

  template <typename T>
  T get(const std::string& key, const T& defaultValue) const {
    auto val = get(key);
    if (val.hasValue()) {
      T ret;
      try {
        ret = folly::to<T>(val.value());
      } catch (folly::ConversionError const& e) {
        BOLT_FAIL("Exception in Config get : {}", e.what());
      }
      return ret;
    } else {
      return defaultValue;
    }
  }

  virtual bool isValueExists(const std::string& key) const = 0;

  virtual const std::unordered_map<std::string, std::string>& values() const {
    BOLT_UNSUPPORTED("method values() is not supported by this config");
  }

  virtual std::unordered_map<std::string, std::string> valuesCopy() const {
    BOLT_UNSUPPORTED("method valuesCopy() is not supported by this config");
  }
};

namespace core {

class MemConfig : public Config {
 public:
  explicit MemConfig(const std::unordered_map<std::string, std::string>& values)
      : values_(values) {}

  explicit MemConfig() : values_{} {}

  explicit MemConfig(std::unordered_map<std::string, std::string>&& values)
      : values_(std::move(values)) {}

  folly::Optional<std::string> get(const std::string& key) const override;

  bool isValueExists(const std::string& key) const override;

  const std::unordered_map<std::string, std::string>& values() const override {
    return values_;
  }

  std::unordered_map<std::string, std::string> valuesCopy() const override {
    return values_;
  }

 private:
  std::unordered_map<std::string, std::string> values_;
};

/// In-memory config allowing changing properties at runtime.
class MemConfigMutable : public Config {
 public:
  explicit MemConfigMutable(
      const std::unordered_map<std::string, std::string>& values)
      : values_(values) {}

  explicit MemConfigMutable() : values_{} {}

  explicit MemConfigMutable(
      std::unordered_map<std::string, std::string>&& values)
      : values_(std::move(values)) {}

  folly::Optional<std::string> get(const std::string& key) const override;

  bool isValueExists(const std::string& key) const override;

  const std::unordered_map<std::string, std::string>& values() const override {
    BOLT_UNSUPPORTED(
        "Mutable config cannot return unprotected reference to values.");
    return *values_.rlock();
  }

  std::unordered_map<std::string, std::string> valuesCopy() const override {
    return *values_.rlock();
  }

  /// Adds or replaces value at the given key. Can be used by debugging or
  /// testing code.
  void setValue(const std::string& key, const std::string& value) {
    (*values_.wlock())[key] = value;
  }

 private:
  folly::Synchronized<std::unordered_map<std::string, std::string>> values_;
};

} // namespace core
} // namespace bytedance::bolt
