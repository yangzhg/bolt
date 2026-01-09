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

#include "bolt/type/filter/FloatingPointRange.h"
#include <fmt/format.h>
namespace bytedance::bolt::common {

template <>
folly::dynamic FloatingPointRange<float>::serialize() const {
  auto obj = AbstractRange::serializeBase("FloatRange");
  obj["lower"] = lower_;
  obj["upper"] = upper_;
  return obj;
}

template <>
folly::dynamic FloatingPointRange<double>::serialize() const {
  auto obj = AbstractRange::serializeBase("DoubleRange");
  obj["lower"] = lower_;
  obj["upper"] = upper_;
  return obj;
}

} // namespace bytedance::bolt::common
