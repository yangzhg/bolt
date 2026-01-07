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

#include "bolt/exec/meta/MetaRowSorter.h"
#include "bolt/exec/meta/MetaTypeUtils.h"
namespace bytedance::bolt::exec {

// TODO: Refine the code
// We can use marco to reduce duplicated code and get a more genernal algorithm

/// @brief An utility function for dispatch types to a Sorter with all key
/// information
///
/// @tparam Types   types holder for types deduction
/// @tparam Arg   sort data arguments wrapper which contains RowContainer, Sort
/// Algorithm, Rows and etc.
/// @param k    the first TypeKind parameter
/// @param ...ks varidic TypeKind parameters
template <typename Arg, typename Types>
void TypesDispatcher(
    Arg& arg,
    const Types types,
    std::same_as<TypeKind> auto k,
    std::same_as<TypeKind> auto... ks) {
  if (k == TypeKind::VARCHAR) {
    typename TypeKindConcat<Types, TypeKindHolder<TypeKind::VARCHAR>>::type
        typesInfo{};
    if constexpr (sizeof...(ks) > 0) {
      TypesDispatcher(arg, typesInfo, ks...);
    } else {
      MetaRowContainerSorter<Arg, decltype(typesInfo)> app{arg};
      app.apply();
    }
  } else if (k == TypeKind::BIGINT) {
    typename TypeKindConcat<Types, TypeKindHolder<TypeKind::BIGINT>>::type
        typesInfo{};
    if constexpr (sizeof...(ks) > 0) {
      TypesDispatcher(arg, typesInfo, ks...);
    } else {
      MetaRowContainerSorter<Arg, decltype(typesInfo)> app{arg};
      app.apply();
    }
  } else if (k == TypeKind::DOUBLE) {
    typename TypeKindConcat<Types, TypeKindHolder<TypeKind::DOUBLE>>::type
        typesInfo{};
    if constexpr (sizeof...(ks) > 0) {
      TypesDispatcher(arg, typesInfo, ks...);
    } else {
      MetaRowContainerSorter<Arg, decltype(typesInfo)> app{arg};
      app.apply();
    }
  } else if (k == TypeKind::INTEGER) {
    typename TypeKindConcat<Types, TypeKindHolder<TypeKind::INTEGER>>::type
        typesInfo{};
    if constexpr (sizeof...(ks) > 0) {
      TypesDispatcher(arg, typesInfo, ks...);
    } else {
      MetaRowContainerSorter<Arg, decltype(typesInfo)> app{arg};
      app.apply();
    }
  } else if (k == TypeKind::BOOLEAN) {
    typename TypeKindConcat<Types, TypeKindHolder<TypeKind::BOOLEAN>>::type
        typesInfo{};
    if constexpr (sizeof...(ks) > 0) {
      TypesDispatcher(arg, typesInfo, ks...);
    } else {
      // finally, we got the  TEMPLATE PARAMETERS(types info)
      // now pass data argument to the appler and apply() what you want.
      MetaRowContainerSorter<Arg, decltype(typesInfo)> app{arg};
      app.apply();
    }
  } else if (k == TypeKind::TINYINT) {
    typename TypeKindConcat<Types, TypeKindHolder<TypeKind::TINYINT>>::type
        typesInfo{};
    if constexpr (sizeof...(ks) > 0) {
      TypesDispatcher(arg, typesInfo, ks...);
    } else {
      MetaRowContainerSorter<Arg, decltype(typesInfo)> app{arg};
      app.apply();
    }
  } else if (k == TypeKind::SMALLINT) {
    typename TypeKindConcat<Types, TypeKindHolder<TypeKind::SMALLINT>>::type
        typesInfo{};
    if constexpr (sizeof...(ks) > 0) {
      TypesDispatcher(arg, typesInfo, ks...);
    } else {
      MetaRowContainerSorter<Arg, decltype(typesInfo)> app{arg};
      app.apply();
    }
  } else if (k == TypeKind::REAL) {
    typename TypeKindConcat<Types, TypeKindHolder<TypeKind::REAL>>::type
        typesInfo{};
    if constexpr (sizeof...(ks) > 0) {
      TypesDispatcher(arg, typesInfo, ks...);
    } else {
      MetaRowContainerSorter<Arg, decltype(typesInfo)> app{arg};
      app.apply();
    }
  } else if (k == TypeKind::VARBINARY) {
    typename TypeKindConcat<Types, TypeKindHolder<TypeKind::VARBINARY>>::type
        typesInfo{};
    if constexpr (sizeof...(ks) > 0) {
      TypesDispatcher(arg, typesInfo, ks...);
    } else {
      MetaRowContainerSorter<Arg, decltype(typesInfo)> app{arg};
      app.apply();
    }
  } else if (k == TypeKind::TIMESTAMP) {
    typename TypeKindConcat<Types, TypeKindHolder<TypeKind::TIMESTAMP>>::type
        typesInfo{};
    if constexpr (sizeof...(ks) > 0) {
      TypesDispatcher(arg, typesInfo, ks...);
    } else {
      MetaRowContainerSorter<Arg, decltype(typesInfo)> app{arg};
      app.apply();
    }
  }
} // ~Dispatcher

} // namespace bytedance::bolt::exec
