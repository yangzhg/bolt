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

#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
namespace bytedance::bolt::connector::hive {

std::string trim(const std::string& str);

std::vector<std::string> splitByComma(const std::string& str);

bool startsWith(const std::string& str, const std::string& prefix);

bool endsWith(const std::string& str, const std::string& postfix);

std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>>
getSequenceGroupInfo(
    const std::unordered_map<std::string, std::string>& tableParameters,
    const std::vector<std::string>& names);

std::vector<std::string> getSequenceGroupKeys(
    std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>>&
        sequenceGroupInfo);

std::vector<int> getIndicesFromNames(
    const std::vector<std::string>& names,
    const std::unordered_map<std::string, int>& nameIdxMap);

std::vector<int> getMappedIndices(
    const std::vector<int>& source,
    const std::vector<int>& target);
} // namespace bytedance::bolt::connector::hive