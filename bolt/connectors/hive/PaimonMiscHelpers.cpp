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

#include "bolt/connectors/hive/PaimonMiscHelpers.h"

#include <algorithm>
#include <cstring>
#include <sstream>
#include "bolt/connectors/hive/PaimonConstants.h"
namespace bytedance::bolt::connector::hive {

std::string trim(const std::string& str) {
  auto start = std::find_if_not(str.begin(), str.end(), ::isspace);
  auto end = std::find_if_not(str.rbegin(), str.rend(), ::isspace).base();
  return (start < end ? std::string(start, end) : "");
}

std::vector<std::string> splitByComma(const std::string& str) {
  std::vector<std::string> result;
  std::stringstream ss(str);
  std::string item;

  while (std::getline(ss, item, ',')) {
    result.push_back(trim(item));
  }

  return result;
}

bool startsWith(const std::string& str, const std::string& prefix) {
  if (str.size() < prefix.size())
    return false;

  return std::equal(prefix.begin(), prefix.end(), str.begin());
}

bool endsWith(const std::string& str, const std::string& postfix) {
  if (str.size() < postfix.size())
    return false;

  return std::equal(postfix.rbegin(), postfix.rend(), str.rbegin());
}

std::vector<std::string> filterFields(
    std::unordered_set<std::string>& namesSet,
    std::vector<std::string>& fieldNames) {
  std::vector<std::string> result;
  for (const auto& fieldName : fieldNames) {
    if (namesSet.find(fieldName) != namesSet.end()) {
      result.push_back(fieldName);
    }
  }
  return result;
}

std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>>
getSequenceGroupInfo(
    const std::unordered_map<std::string, std::string>& tableParameters,
    const std::vector<std::string>& names) {
  std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>>
      result;
  std::unordered_set<std::string> namesSet(names.begin(), names.end());

  for (const auto& [key, value] : tableParameters) {
    if (!startsWith(key, connector::paimon::kPartialUpdateKeyPrefix))
      continue;

    if (!endsWith(key, connector::paimon::kPartialUpdateSequenceGroup))
      continue;

    auto nonFilteredValueNames = splitByComma(value);
    auto valueNames = filterFields(namesSet, nonFilteredValueNames);
    if (valueNames.empty())
      continue;

    auto prefixSize = strlen(connector::paimon::kPartialUpdateKeyPrefix);
    auto postfixSize = strlen(connector::paimon::kPartialUpdateSequenceGroup);
    auto fieldsWithComma =
        key.substr(prefixSize, key.size() - prefixSize - postfixSize);
    auto fields = splitByComma(fieldsWithComma);
    result.emplace_back(fields, valueNames);
  }

  return result;
}

std::vector<std::string> getSequenceGroupKeys(
    std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>>&
        sequenceGroupInfo) {
  std::unordered_set<std::string> set;
  for (const auto& [keys, values] : sequenceGroupInfo) {
    for (const auto& key : keys) {
      set.insert(key);
    }
  }
  return std::vector<std::string>(set.begin(), set.end());
}

std::vector<int> getIndicesFromNames(
    const std::vector<std::string>& names,
    const std::unordered_map<std::string, int>& nameIdxMap) {
  std::vector<int> result;
  result.reserve(names.size());
  for (const auto& name : names) {
    result.push_back(nameIdxMap.at(name));
  }
  return result;
}

std::vector<int> getMappedIndices(
    const std::vector<int>& source,
    const std::vector<int>& target) {
  std::unordered_map<int, int> map;
  for (int i = 0; i < target.size(); i++) {
    map[target[i]] = i;
  }

  std::vector<int> result;
  result.reserve(source.size());
  for (const auto i : source) {
    if (map.find(i) != map.end()) {
      result.push_back(map[i]);
    }
  }

  return result;
}

} // namespace bytedance::bolt::connector::hive