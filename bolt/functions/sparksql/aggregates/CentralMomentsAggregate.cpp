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

#include "bolt/functions/sparksql/aggregates/CentralMomentsAggregate.h"
#include "bolt/functions/lib/aggregates/CentralMomentsAggregatesBase.h"
namespace bytedance::bolt::functions::aggregate::sparksql {

namespace {
template <bool nullOnDivideByZero>
struct SkewnessResultAccessor {
  static bool hasResult(const CentralMomentsAccumulator& accumulator) {
    if constexpr (nullOnDivideByZero) {
      return accumulator.count() >= 1 && accumulator.m2() != 0;
    }
    return accumulator.count() >= 1;
  }
  static double result(const CentralMomentsAccumulator& accumulator) {
    if (accumulator.m2() == 0) {
      BOLT_CHECK(
          !nullOnDivideByZero,
          "NaN is returned only when m2 is 0 and nullOnDivideByZero is false.");
      return std::numeric_limits<double>::quiet_NaN();
    }
    return std::sqrt(accumulator.count()) * accumulator.m3() /
        std::pow(accumulator.m2(), 1.5);
  }
};

// Calculate the kurtosis value from m2, count and m4.
//
// @tparam nullOnDivideByZero If true, return NULL instead of NaN when dividing
// by zero during the calculating.
template <bool nullOnDivideByZero>
struct KurtosisResultAccessor {
  static bool hasResult(const CentralMomentsAccumulator& accumulator) {
    if constexpr (nullOnDivideByZero) {
      return accumulator.count() >= 1 && accumulator.m2() != 0;
    }
    return accumulator.count() >= 1;
  }
  static double result(const CentralMomentsAccumulator& accumulator) {
    if (accumulator.m2() == 0) {
      BOLT_CHECK(
          !nullOnDivideByZero,
          "NaN is returned only when m2 is 0 and nullOnDivideByZero is false.");
      return std::numeric_limits<double>::quiet_NaN();
    }
    double count = accumulator.count();
    double m2 = accumulator.m2();
    double m4 = accumulator.m4();
    return count * m4 / (m2 * m2) - 3.0;
  }
};

template <template <bool> class TResultAccessor>
exec::AggregateRegistrationResult registerCentralMoments(
    const std::string& name,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  std::vector<std::string> inputTypes = {
      "smallint", "integer", "bigint", "real", "double"};
  for (const auto& inputType : inputTypes) {
    signatures.push_back(
        exec::AggregateFunctionSignatureBuilder()
            .returnType("double")
            .intermediateType(CentralMomentsIntermediateResult::type())
            .argumentType(inputType)
            .build());
  }

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
        BOLT_CHECK_LE(
            argTypes.size(), 1, "{} takes at most one argument", name);
        const auto& inputType = argTypes[0];
        bool nullOnDivideByZero = !config.sparkLegacyStatisticalAggregate();
        if (exec::isRawInput(step)) {
          switch (inputType->kind()) {
            case TypeKind::SMALLINT:
              if (nullOnDivideByZero) {
                return std::make_unique<CentralMomentsAggregatesBase<
                    int16_t,
                    TResultAccessor<true>>>(resultType);
              } else {
                return std::make_unique<CentralMomentsAggregatesBase<
                    int16_t,
                    TResultAccessor<false>>>(resultType);
              }
            case TypeKind::INTEGER:
              if (nullOnDivideByZero) {
                return std::make_unique<CentralMomentsAggregatesBase<
                    int32_t,
                    TResultAccessor<true>>>(resultType);
              } else {
                return std::make_unique<CentralMomentsAggregatesBase<
                    int32_t,
                    TResultAccessor<false>>>(resultType);
              }
            case TypeKind::BIGINT:
              if (nullOnDivideByZero) {
                return std::make_unique<CentralMomentsAggregatesBase<
                    int64_t,
                    TResultAccessor<true>>>(resultType);
              } else {
                return std::make_unique<CentralMomentsAggregatesBase<
                    int64_t,
                    TResultAccessor<false>>>(resultType);
              }
            case TypeKind::DOUBLE:
              if (nullOnDivideByZero) {
                return std::make_unique<CentralMomentsAggregatesBase<
                    double,
                    TResultAccessor<true>>>(resultType);
              } else {
                return std::make_unique<CentralMomentsAggregatesBase<
                    double,
                    TResultAccessor<false>>>(resultType);
              }
            case TypeKind::REAL:
              if (nullOnDivideByZero) {
                return std::make_unique<
                    CentralMomentsAggregatesBase<float, TResultAccessor<true>>>(
                    resultType);
              } else {
                return std::make_unique<CentralMomentsAggregatesBase<
                    float,
                    TResultAccessor<false>>>(resultType);
              }
            default:
              BOLT_UNSUPPORTED(
                  "Unsupported input type: {}. "
                  "Expected SMALLINT, INTEGER, BIGINT, DOUBLE or REAL.",
                  inputType->toString())
          }
        } else {
          checkAccumulatorRowType(
              inputType,
              "Input type for final aggregation must be "
              "(count:bigint, m1:double, m2:double, m3:double, m4:double) struct");
          if (nullOnDivideByZero) {
            return std::make_unique<CentralMomentsAggregatesBase<
                int64_t /*unused*/,
                TResultAccessor<true>>>(resultType);
          } else {
            return std::make_unique<CentralMomentsAggregatesBase<
                int64_t /*unused*/,
                TResultAccessor<false>>>(resultType);
          }
        }
      },
      withCompanionFunctions,
      overwrite);
}
} // namespace

void registerCentralMomentsAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerCentralMoments<SkewnessResultAccessor>(
      prefix + "skewness", withCompanionFunctions, overwrite);
  registerCentralMoments<KurtosisResultAccessor>(
      prefix + "kurtosis", withCompanionFunctions, overwrite);
}

} // namespace bytedance::bolt::functions::aggregate::sparksql
