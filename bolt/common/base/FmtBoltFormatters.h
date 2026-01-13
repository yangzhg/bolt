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
#include <fmt/format.h>

#include "bolt/common/memory/MemoryAllocationListener.h"
#include "bolt/shuffle/sparksql/Options.h"

template <>
struct fmt::formatter<bytedance::bolt::shuffle::sparksql::PartitionWriterType>
    : fmt::formatter<std::string_view> {
  auto format(
      bytedance::bolt::shuffle::sparksql::PartitionWriterType c,
      format_context& ctx) const {
    switch (c) {
      case bytedance::bolt::shuffle::sparksql::kLocal:
        return fmt::formatter<std::string_view>::format("kLocal", ctx);
      case bytedance::bolt::shuffle::sparksql::kCeleborn:
        return fmt::formatter<std::string_view>::format("kCeleborn", ctx);
      default:
        return fmt::format_to(ctx.out(), "unknown[{}]", static_cast<int>(c));
    }
  }
};

template <>
struct fmt::formatter<
    bytedance::bolt::memory::MemoryAllocationListener::ListenMode>
    : fmt::formatter<std::string_view> {
  auto format(
      bytedance::bolt::memory::MemoryAllocationListener::ListenMode c,
      format_context& ctx) const {
    switch (c) {
      case bytedance::bolt::memory::MemoryAllocationListener::ListenMode::
          kDisable:
        return fmt::formatter<std::string_view>::format("kDisable", ctx);
      case bytedance::bolt::memory::MemoryAllocationListener::ListenMode::
          kOnlySingle:
        return fmt::formatter<std::string_view>::format("kOnlySingle", ctx);
      case bytedance::bolt::memory::MemoryAllocationListener::ListenMode::
          kOnlyAccumulative:
        return fmt::formatter<std::string_view>::format(
            "kOnlyAccumulative", ctx);
      case bytedance::bolt::memory::MemoryAllocationListener::ListenMode::kBoth:
        return fmt::formatter<std::string_view>::format("kBoth", ctx);
      default:
        return fmt::format_to(ctx.out(), "unknown[{}]", static_cast<int>(c));
    }
  }
};
