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

#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/AggregateFunction.h"

#include <vector>
namespace bytedance::bolt::connector::paimon {

template <typename T>
class FieldSumAgg : public AggregateFunction {
  T sum;

 public:
  FieldSumAgg() : sum(0) {}

  void add(VectorPtr value, size_t rowIndex) override {
    if (value->isNullAt(rowIndex))
      return;

    auto val = value->asFlatVector<T>()->valueAt(rowIndex);
    VLOG(2) << "Sum:" << (double)sum << "+" << (double)val;
    sum += val;
  }

  void appendResult(VectorPtr dest) override {
    VLOG(2) << "Appending sum:" << (double)sum;
    dest->asFlatVector<T>()->set(dest->size() - 1, sum);
    sum = 0;
  }
};

std::shared_ptr<AggregateFunction> createFieldSumAgg(const TypePtr& type) {
  auto typeKind = type->kind();
  switch (typeKind) {
    case TypeKind::TINYINT:
      return std::make_shared<FieldSumAgg<int8_t>>();
    case TypeKind::SMALLINT:
      return std::make_shared<FieldSumAgg<int16_t>>();
    case TypeKind::INTEGER:
      return std::make_shared<FieldSumAgg<int32_t>>();
    case TypeKind::BIGINT:
      return std::make_shared<FieldSumAgg<int64_t>>();
    case TypeKind::HUGEINT:
      return std::make_shared<FieldSumAgg<int128_t>>();
    case TypeKind::REAL:
      return std::make_shared<FieldSumAgg<float>>();
    case TypeKind::DOUBLE:
      return std::make_shared<FieldSumAgg<double>>();
    default:
      BOLT_UNSUPPORTED(
          "Unsupported type for sum: {}", mapTypeKindToName(typeKind));
  }
}

} // namespace bytedance::bolt::connector::paimon
