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

#include "bolt/functions/sparksql/aggregates/BitwiseXorAggregate.h"

#include "bolt/functions/lib/aggregates/BitwiseAggregateBase.h"
namespace bytedance::bolt::functions::aggregate::sparksql {

namespace {

template <typename T>
class BitwiseXorAggregate : public BitwiseAggregateBase<T> {
 public:
  explicit BitwiseXorAggregate(TypePtr resultType)
      : aggregate::BitwiseAggregateBase<T>(
            resultType,
            /* initialValue = */ 0) {}

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    aggregate::SimpleNumericAggregate<T, T, T>::template updateGroups<true>(
        groups,
        rows,
        args[0],
        [](T& result, T value) { result ^= value; },
        mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    aggregate::SimpleNumericAggregate<T, T, T>::updateOneGroup(
        group,
        rows,
        args[0],
        [](T& result, T value) { result ^= value; },
        [](T& result, T value, int n) {
          if ((n & 1) == 1) {
            result ^= value;
          }
        },
        mayPushdown,
        this->initialValue_);
  }
};

} // namespace

exec::AggregateRegistrationResult registerBitwiseXorAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  return functions::aggregate::registerBitwise<BitwiseXorAggregate>(
      prefix + "bit_xor", withCompanionFunctions, overwrite);
}

} // namespace bytedance::bolt::functions::aggregate::sparksql
