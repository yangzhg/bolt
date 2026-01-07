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

#include <DataSketches/hll.hpp>
#include "bolt/functions/Macros.h"
#include "bolt/functions/Registerer.h"
namespace bytedance::bolt::functions {

using HLLSketch = datasketches::hll_sketch;
namespace {
template <typename T>
struct SketchHLLEstimate {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool callNullable(
      double& out,
      const arg_type<Varbinary>* varbinaryInput,
      const arg_type<bool>& isStable) {
    if (varbinaryInput == nullptr || varbinaryInput->size() == 0) {
      return false;
    }
    auto sketch =
        HLLSketch::deserialize(varbinaryInput->data(), varbinaryInput->size());
    out = isStable ? sketch.get_composite_estimate() : sketch.get_estimate();
    out = std::round(out);
    return true;
  }

  FOLLY_ALWAYS_INLINE bool callNullable(
      double& out,
      const arg_type<Varbinary>* varbinaryInput) {
    if (varbinaryInput == nullptr || varbinaryInput->size() == 0) {
      return false;
    }
    auto sketch =
        HLLSketch::deserialize(varbinaryInput->data(), varbinaryInput->size());
    out = sketch.get_estimate();
    return true;
  }
};
} // namespace
} // namespace bytedance::bolt::functions