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

#include <string>
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/Registerer.h"
#include "bolt/functions/lib/MapConcat.h"
#include "bolt/functions/lib/MapFromEntries.h"
#include "bolt/functions/prestosql/MultimapFromEntries.h"
namespace bytedance::bolt::functions {

void registerMapFunctions(const std::string& prefix) {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_map_filter, prefix + "map_filter");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_transform_keys, prefix + "transform_keys");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_transform_values, prefix + "transform_values");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_map, prefix + "map");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_map_entries, prefix + "map_entries");
  registerMapFromEntriesFunction(prefix + "map_from_entries", true);

  BOLT_REGISTER_VECTOR_FUNCTION(udf_map_keys, prefix + "map_keys");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_map_values, prefix + "map_values");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_map_zip_with, prefix + "map_zip_with");

  BOLT_REGISTER_VECTOR_FUNCTION(udf_all_keys_match, prefix + "all_keys_match");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_any_keys_match, prefix + "any_keys_match");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_no_keys_match, prefix + "no_keys_match");

  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_any_values_match, prefix + "any_values_match");
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_no_values_match, prefix + "no_values_match");

  registerMapConcatFunction(prefix + "map_concat");

  registerFunction<
      MultimapFromEntriesFunction,
      Map<Generic<T1>, Array<Generic<T2>>>,
      Array<Row<Generic<T1>, Generic<T2>>>>({prefix + "multimap_from_entries"});

  BOLT_REGISTER_VECTOR_FUNCTION(udf_map_update, prefix + "map_update");

  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_map_filter_keys, prefix + "map_filter_keys");
}

void registerMapAllowingDuplicates(
    const std::string& name,
    const std::string& prefix) {
  BOLT_REGISTER_VECTOR_FUNCTION(udf_map_allow_duplicates, prefix + name);
}
} // namespace bytedance::bolt::functions
