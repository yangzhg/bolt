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

#include "bolt/expression/RegisterSpecialForm.h"
#include "bolt/functions/Registerer.h"
#include "bolt/functions/lib/IsNull.h"
#include "bolt/functions/lib/Size.h"
#include "bolt/functions/prestosql/Cardinality.h"
namespace bytedance::bolt::functions {
extern void registerSubscriptFunction(
    const std::string& name,
    bool enableCaching);
extern void registerElementAtFunction(
    const std::string& name,
    bool enableCaching);

// Special form functions don't have any prefix.
void registerAllSpecialFormGeneralFunctions(const std::string& prefix) {
  exec::registerFunctionCallToSpecialForms();
  BOLT_REGISTER_VECTOR_FUNCTION(udf_in, prefix + "in");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_concat_row, prefix + "row_constructor");
  registerIsNullFunction(prefix + "is_null");
  registerIsNullFunction(prefix + "isnull");
  registerIsNotNullFunction(prefix + "is_not_null");
  registerIsNotNullFunction(prefix + "isnotnull");
}

void registerGeneralFunctions(const std::string& prefix) {
  registerSubscriptFunction(prefix + "subscript", true);
  registerElementAtFunction(prefix + "element_at", true);

  BOLT_REGISTER_VECTOR_FUNCTION(udf_transform, prefix + "transform");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_reduce, prefix + "reduce");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_array_filter, prefix + "filter");

  BOLT_REGISTER_VECTOR_FUNCTION(udf_least, prefix + "least");
  BOLT_REGISTER_VECTOR_FUNCTION(udf_greatest, prefix + "greatest");

  registerFunction<CardinalityFunction, int64_t, Array<Any>>(
      {prefix + "cardinality"});
  registerFunction<CardinalityFunction, int64_t, Map<Any, Any>>(
      {prefix + "cardinality"});

  // Register size functions
  registerSize(prefix + "size");

  registerAllSpecialFormGeneralFunctions("");
  registerAllSpecialFormGeneralFunctions(prefix);
}

} // namespace bytedance::bolt::functions
