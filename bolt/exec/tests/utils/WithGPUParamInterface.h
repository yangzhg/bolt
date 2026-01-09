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

#include <gtest/gtest.h>
#include <functional>
namespace bytedance::bolt::exec::test {

struct GPUParam {
  bool useGPU = false;
};

#if defined BOLT_HAS_CUDF && BOLT_HAS_CUDF == 1
inline constexpr bool isGPUEnabled = true;
#else
inline constexpr bool isGPUEnabled = false;
#endif

template <bool EnableGPU = isGPUEnabled>
class WithGPUParamInterface : public testing::WithParamInterface<GPUParam> {
 public:
  static constexpr auto testValues() {
    if constexpr (EnableGPU) {
      return testing::Values(
          GPUParam{.useGPU = false}, GPUParam{.useGPU = true});
    } else {
      return testing::Values(GPUParam{.useGPU = false});
    }
  }

  static constexpr auto testNamer() {
    return [](const testing::TestParamInfo<GPUParam>& info) {
      return info.param.useGPU ? "GPU" : "CPU";
    };
  }
};

#define INSTANTIATE_GPU_TEST_SUITE_P(InstantiationName, TestSuiteName) \
  INSTANTIATE_TEST_SUITE_P(                                            \
      InstantiationName,                                               \
      TestSuiteName,                                                   \
      TestSuiteName::WithGPUParamInterface::testValues(),              \
      TestSuiteName::WithGPUParamInterface::testNamer())

} // namespace bytedance::bolt::exec::test
