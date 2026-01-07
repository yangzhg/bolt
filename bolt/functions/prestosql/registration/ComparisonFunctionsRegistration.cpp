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

#include "bolt/functions/Registerer.h"
#include "bolt/functions/lib/RegistrationHelpers.h"
#include "bolt/functions/prestosql/Comparisons.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt::functions {

void registerComparisonFunctions(const std::string& prefix) {
  // Comparison functions also need TimestampWithTimezoneType,
  // independent of DateTimeFunctions
  registerTimestampWithTimeZoneType();

  registerNonSimdizableScalar<EqFunction, bool>({prefix + "eq"});
  BOLT_REGISTER_VECTOR_FUNCTION(udf_simd_comparison_eq, prefix + "eq");
  registerFunction<EqFunction, bool, Generic<T1>, Generic<T1>>({prefix + "eq"});

  registerNonSimdizableScalar<NeqFunction, bool>({prefix + "neq"});
  BOLT_REGISTER_VECTOR_FUNCTION(udf_simd_comparison_neq, prefix + "neq");
  registerFunction<NeqFunction, bool, Generic<T1>, Generic<T1>>(
      {prefix + "neq"});

  registerNonSimdizableScalar<LtFunction, bool>({prefix + "lt"});
  BOLT_REGISTER_VECTOR_FUNCTION(udf_simd_comparison_lt, prefix + "lt");

  registerNonSimdizableScalar<GtFunction, bool>({prefix + "gt"});
  BOLT_REGISTER_VECTOR_FUNCTION(udf_simd_comparison_gt, prefix + "gt");

  registerNonSimdizableScalar<LteFunction, bool>({prefix + "lte"});
  BOLT_REGISTER_VECTOR_FUNCTION(udf_simd_comparison_lte, prefix + "lte");

  registerNonSimdizableScalar<GteFunction, bool>({prefix + "gte"});
  BOLT_REGISTER_VECTOR_FUNCTION(udf_simd_comparison_gte, prefix + "gte");

  registerBinaryScalar<DistinctFromFunction, bool>({prefix + "distinct_from"});

  registerFunction<BetweenFunction, bool, int8_t, int8_t, int8_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, int16_t, int16_t, int16_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, int32_t, int32_t, int32_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, int64_t, int64_t, int64_t>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, double, double, double>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, float, float, float>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, Varchar, Varchar, Varchar>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, Date, Date, Date>(
      {prefix + "between"});
  registerFunction<BetweenFunction, bool, Timestamp, Timestamp, Timestamp>(
      {prefix + "between"});

  BOLT_REGISTER_VECTOR_FUNCTION(udf_decimal_between, prefix + "between");
}

} // namespace bytedance::bolt::functions
