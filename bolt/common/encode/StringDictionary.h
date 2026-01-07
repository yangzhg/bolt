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

#include <cstdint>
#include <optional>

#include <folly/container/F14Map.h>
namespace bytedance::bolt {

template <typename T = uint64_t>
class StringDictionary {
 public:
  StringDictionary() {
    stringToId_.reserve(128);
    idToString_.reserve(128);
  }

  StringDictionary(const StringDictionary& other) = delete;
  StringDictionary(StringDictionary&& other) = delete;
  void operator=(const StringDictionary& other) = delete;
  void operator=(StringDictionary&& other) = delete;

  T storeString(const std::string& str) {
    auto it = stringToId_.find(str);
    if (it != stringToId_.end()) {
      return it->second;
    }
    auto id = ++lastId_;
    stringToId_[str] = id;
    idToString_[id] = str;
    return id;
  }

  std::optional<std::string> getString(const T& id) const {
    auto it = idToString_.find(id);
    if (it != idToString_.end()) {
      return it->second;
    } else {
      return std::nullopt;
    }
  }

  uint32_t mapSize() const {
    return lastId_;
  }

 private:
  folly::F14FastMap<std::string, T> stringToId_;
  folly::F14FastMap<T, std::string> idToString_;
  T lastId_{0};
};

} // namespace bytedance::bolt