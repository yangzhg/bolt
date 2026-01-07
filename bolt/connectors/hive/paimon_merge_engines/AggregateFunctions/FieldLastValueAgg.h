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

class FieldLastValueAgg : public AggregateFunction {
  VectorPtr value_;
  size_t rowIndex_;

 public:
  FieldLastValueAgg() {
    value_ = nullptr;
  }

  void add(VectorPtr value, size_t rowIndex) override {
    value_ = value;
    rowIndex_ = rowIndex;
  }

  void appendResult(VectorPtr dest) override {
    if (value_) {
      dest->copy(value_.get(), dest->size() - 1, rowIndex_, 1);
    }

    value_ = nullptr;
  }
};

std::shared_ptr<AggregateFunction> createFieldLastValueAgg() {
  return std::make_shared<FieldLastValueAgg>();
}

} // namespace bytedance::bolt::connector::paimon
