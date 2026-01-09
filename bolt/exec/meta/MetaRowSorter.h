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

#include "bolt/exec/HybridSorter.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/meta/MetaRowComparator.h"
#include "bolt/exec/meta/MetaRowSorterApi.h"
#include "bolt/exec/meta/MetaTypeUtils.h"
#include "bolt/type/StringView.h"
#include "common/base/CompareFlags.h"

#include <vector>
namespace bytedance::bolt::exec {

/// @brief A wrapper for passing sorting arguments
/// @tparam Rows: A collection of pointers to the rows of RowContainer
/// @param container: RowContainer
/// @param container: cols:  column info of the keys sorted by
/// @param container: flags: sort flags of the keys sorted by
template <RowsIterable Rows>
struct SorterDataArgs {
  SorterDataArgs(
      HybridSorter& sortAlgo,
      Rows& rows,
      RowContainer& container,
      const std::vector<column_index_t>& sortKeysIndices,
      const std::vector<CompareFlags>& compareFlags)
      : sortAlgo(sortAlgo),
        rows(rows),
        container(container),
        sortKeysIndices(sortKeysIndices),
        compareFlags(compareFlags) {}
  HybridSorter& sortAlgo;
  Rows& rows;
  RowContainer& container;
  const std::vector<column_index_t>& sortKeysIndices;
  const std::vector<CompareFlags>& compareFlags;
};

/// @brief  Generic declaration of CompiledContainerSorter
template <typename DataArgs, typename T, typename...>
class MetaRowContainerSorter;

/// @brief This is used for sorting rows(Rowontainer/SpillRows)
///        With types information(compiling stage)
///
/// @tparam DataArgs:  arguments wrapper with RowContainer data, sort flags in
/// it.
/// @tparam Kinds: types of the keys in a row
template <typename DataArgs, TypeKind... Kinds>
class MetaRowContainerSorter<DataArgs, TypeKindHolder<Kinds...>> {
  // static constexpr TypeKind Kinds[] = {KINDS...};
 public:
  // () : dataPtr(nullptr) {} // for dummy
  explicit MetaRowContainerSorter(DataArgs& arg) : sortArg_(arg) {}

  void apply() {
    MetaRowComparator<Kinds...> cmp_(
        sortArg_.container, sortArg_.sortKeysIndices, sortArg_.compareFlags);

    sortArg_.sortAlgo.sort(sortArg_.rows.begin(), sortArg_.rows.end(), cmp_);
  }

 private:
  DataArgs& sortArg_;
};

} // namespace bytedance::bolt::exec
