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
namespace bytedance::bolt::memory {

class MemoryUtils final {
 public:
  static constexpr int64_t kInvalidRssSize = 0;

  static int64_t getProcessRss();

  static int64_t getOnHeapMemUsed();

  using getOnHeapMemUsedImplPtr = int64_t (*)();
  inline static getOnHeapMemUsedImplPtr hook{nullptr};
};

} // namespace bytedance::bolt::memory
