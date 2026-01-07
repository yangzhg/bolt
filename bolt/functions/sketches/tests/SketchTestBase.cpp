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

#include "SketchTestBase.hpp"
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
namespace bytedance::bolt::aggregate::sketches::test {

const std::vector<std::string> SketchTestBase::kFruits = {
    "apple",
    "banana",
    "cherry",
    "dragonfruit",
    "grapefruit",
    "melon",
    "orange",
    "pear",
    "pineapple",
    "unknown fruit with a very long name",
    "watermelon"};

const std::vector<std::string> SketchTestBase::kVegetables = {
    "cucumber",
    "tomato",
    "potato",
    "squash",
    "unknown vegetable with a very long name"};

} // namespace bytedance::bolt::aggregate::sketches::test
