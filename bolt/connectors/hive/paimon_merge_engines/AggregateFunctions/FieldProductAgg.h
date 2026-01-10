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
#include "bolt/functions/sparksql/DecimalUtil.h"
#include "bolt/type/DecimalUtil.h"
namespace bytedance::bolt::connector::paimon {

template <typename T>
class FieldProductAgg : public AggregateFunction {
  T product_;

 public:
  FieldProductAgg() : product_(1) {}
  virtual ~FieldProductAgg() = default;

  void add(VectorPtr value, size_t rowIndex) override {
    if (value->isNullAt(rowIndex))
      return;

    auto val = value->asFlatVector<T>()->valueAt(rowIndex);
    VLOG(2) << "Product:" << (double)product_ << "*" << (double)val;
    product_ *= val;
  }

  void appendResult(VectorPtr dest) override {
    VLOG(2) << "Appending product:" << (double)product_;
    dest->asFlatVector<T>()->set(dest->size() - 1, product_);
    product_ = 1;
  }
};

template <typename T>
class DecimalFieldProductAgg : public AggregateFunction {
  uint8_t scale_;
  TypePtr type_;
  int128_t powerOf10_;
  T product_;

 public:
  DecimalFieldProductAgg(uint8_t scale, TypePtr type)
      : scale_(scale), type_(type) {
    powerOf10_ = bolt::DecimalUtil::getPowersOfTen(scale_);
    product_ = powerOf10_;
  }
  virtual ~DecimalFieldProductAgg() = default;

  void add(VectorPtr value, size_t rowIndex) override {
    if (value->isNullAt(rowIndex))
      return;

    auto val = value->asFlatVector<T>()->valueAt(rowIndex);
    bool overflow = false;
    VLOG(2) << "Product:" << DecimalUtil::toString(product_, type_) << "*"
            << DecimalUtil::toString(val, type_);
    auto result =
        bytedance::bolt::functions::sparksql::DecimalUtil::multiply<int128_t>(
            product_, val, overflow);
    product_ = T(result / powerOf10_);
  }

  void appendResult(VectorPtr dest) override {
    VLOG(2) << "Appending product:" << DecimalUtil::toString(product_, type_);
    dest->asFlatVector<T>()->set(dest->size() - 1, product_);
    product_ = powerOf10_;
  }
};

std::shared_ptr<AggregateFunction> createFieldProductAgg(const TypePtr& type) {
  if (type->isShortDecimal()) {
    auto [precision, scale] = getDecimalPrecisionScale(*type);
    return std::make_shared<DecimalFieldProductAgg<int64_t>>(scale, type);
  } else if (type->isLongDecimal()) {
    auto [precision, scale] = getDecimalPrecisionScale(*type);
    return std::make_shared<DecimalFieldProductAgg<int128_t>>(scale, type);
  }

  auto typeKind = type->kind();
  switch (typeKind) {
    case TypeKind::TINYINT:
      return std::make_shared<FieldProductAgg<int8_t>>();
    case TypeKind::SMALLINT:
      return std::make_shared<FieldProductAgg<int16_t>>();
    case TypeKind::INTEGER:
      return std::make_shared<FieldProductAgg<int32_t>>();
    case TypeKind::BIGINT:
      return std::make_shared<FieldProductAgg<int64_t>>();
    case TypeKind::REAL:
      return std::make_shared<FieldProductAgg<float>>();
    case TypeKind::DOUBLE:
      return std::make_shared<FieldProductAgg<double>>();
    default:
      BOLT_UNSUPPORTED(
          "Unsupported type for product: {}", mapTypeKindToName(typeKind));
  }
}

} // namespace bytedance::bolt::connector::paimon
