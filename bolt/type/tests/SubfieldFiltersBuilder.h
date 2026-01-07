/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include "bolt/type/Subfield.h"
namespace bytedance::bolt::common::test {

using SubfieldFilters =
    std::unordered_map<common::Subfield, std::unique_ptr<common::Filter>>;

class SubfieldFiltersBuilder {
 public:
  SubfieldFiltersBuilder& add(
      const std::string& path,
      std::unique_ptr<common::Filter> filter) {
    filters_[common::Subfield(path)] = std::move(filter);
    return *this;
  }

  SubfieldFilters build() {
    return std::move(filters_);
  }

 private:
  SubfieldFilters filters_;
};

inline SubfieldFilters singleSubfieldFilter(
    const std::string& path,
    std::unique_ptr<common::Filter> filter) {
  return SubfieldFiltersBuilder().add(path, std::move(filter)).build();
}
} // namespace bytedance::bolt::common::test
