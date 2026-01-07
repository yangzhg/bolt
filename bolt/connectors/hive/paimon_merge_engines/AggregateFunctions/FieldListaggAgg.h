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

class FieldListaggAgg : public AggregateFunction {
  std::shared_ptr<std::string> list;
  std::string delimiter;

 public:
  FieldListaggAgg(std::string delimiter_) {
    list = nullptr;
    delimiter = delimiter_;
  }

  void add(VectorPtr value, size_t rowIndex) override {
    if (value->isNullAt(rowIndex))
      return;

    DecodedVector decoded;
    decoded.decode(*value);

    std::string val = decoded.valueAt<StringView>(rowIndex);
    if (!list) {
      list = std::make_shared<std::string>(val);
    } else {
      list->append(delimiter).append(val);
    }
  }

  void appendResult(VectorPtr dest) override {
    if (list)
      dest->asFlatVector<StringView>()->set(
          dest->size() - 1, StringView(*list));
    else
      dest->setNull(dest->size() - 1, true);

    list = nullptr;
  }
};

std::shared_ptr<AggregateFunction> createFieldListaggAgg(
    std::string delimiter,
    const TypePtr& type) {
  if (type == VARCHAR())
    return std::make_shared<FieldListaggAgg>(delimiter);

  BOLT_UNSUPPORTED("Unsupported type for list-agg: {}", type->name());
}

} // namespace bytedance::bolt::connector::paimon
