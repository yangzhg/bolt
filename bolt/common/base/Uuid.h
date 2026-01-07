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

#include <random>
#include <sstream>
#include <string>
namespace bytedance::bolt {

std::string makeUuid();
std::string makeUuid(uint64_t seed);
std::string makeUuid(std::mt19937_64& rng);
std::string makeUuid(uint64_t mostSigBits, uint64_t leastSigBits);
void makeUuid(char* buf);
void makeUuid(char* buf, uint64_t seed);
void makeUuid(char* buf, std::mt19937_64& rng);
void makeUuid(char* buf, uint64_t mostSigBits, uint64_t leastSigBits);
} // namespace bytedance::bolt