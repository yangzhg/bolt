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

#include "bolt/connectors/hive/PaimonRowIterator.h"
#include "bolt/connectors/hive/paimon_merge_engines/AggregateFunctions/AggregateFunction.h"

#include <vector>
namespace bytedance::bolt::connector::paimon {

class FieldBoolOrAgg : public AggregateFunction {
  std::shared_ptr<bool> val;

 public:
  FieldBoolOrAgg() {
    val = nullptr;
  }

  virtual ~FieldBoolOrAgg() = default;

  void add(VectorPtr value, size_t rowIndex) override {
    if (value->isNullAt(rowIndex))
      return;

    if (!val)
      val = std::make_shared<bool>(false);

    *val = *val || value->asFlatVector<bool>()->valueAt(rowIndex);
  }

  void appendResult(VectorPtr dest) override {
    if (val) {
      dest->asFlatVector<bool>()->set(dest->size() - 1, *val);
    } else {
      dest->setNull(dest->size() - 1, true);
    }

    val = nullptr;
  }
};

std::shared_ptr<AggregateFunction> createFieldBoolOrAgg(const TypePtr& type) {
  auto typeKind = type->kind();
  switch (typeKind) {
    case TypeKind::BOOLEAN:
      return std::make_shared<FieldBoolOrAgg>();
    default:
      BOLT_UNSUPPORTED(
          "Unsupported type for sum: {}", mapTypeKindToName(typeKind));
  }
}

} // namespace bytedance::bolt::connector::paimon
