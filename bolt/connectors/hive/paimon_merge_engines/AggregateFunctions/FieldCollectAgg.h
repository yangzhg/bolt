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

template <typename T>
class FieldCollectAgg : public AggregateFunction {
  VectorPtr collection_;
  std::unordered_set<T> distincts;
  bool distinct_;
  bool hasNull_;
  vector_size_t lastOffset_;

 public:
  FieldCollectAgg(bool distinct = false) {
    collection_ = nullptr;
    lastOffset_ = 0;
    distinct_ = distinct;
    hasNull_ = false;
  }
  virtual ~FieldCollectAgg() = default;

  void add(VectorPtr value, size_t rowIndex) override {
    if (value->isNullAt(rowIndex))
      return;

    auto asArray = value->as<ArrayVector>();
    auto elements = asArray->elements();
    BaseVector::flattenVector(elements);
    auto offset = asArray->offsetAt(rowIndex);
    auto size = asArray->sizeAt(rowIndex);

    if (!collection_) {
      collection_ = BaseVector::create(elements->type(), 0, value->pool());
      BaseVector::flattenVector(collection_);
    }

    if (distinct_) {
      auto flat = elements->as<FlatVector<T>>();
      for (vector_size_t i = offset; i < offset + size; ++i) {
        if (flat->isNullAt(i)) {
          if (!hasNull_) {
            hasNull_ = true;
            auto colSize = collection_->size();
            collection_->resize(colSize + 1);
            collection_->setNull(colSize, true);
          }
        } else {
          auto element = flat->valueAt(i);
          if (distincts.find(element) == distincts.end()) {
            distincts.insert(element);
            auto colSize = collection_->size();
            collection_->resize(colSize + 1);
            collection_->as<FlatVector<T>>()->set(colSize, element);
          }
        }
      }
    } else {
      auto start = collection_->size();
      collection_->resize(start + size);
      collection_->copy(elements.get(), start, offset, size);
    }
  }

  void appendResult(VectorPtr dest) override {
    if (collection_) {
      auto destArray = dest->as<ArrayVector>();
      auto destSize = destArray->size();

      auto destElems = destArray->elements();
      auto start = destElems->size();
      auto srcSize = collection_->size();
      destElems->resize(start + srcSize);
      destElems->copy(collection_.get(), start, 0, srcSize);

      destArray->setOffsetAndSize(destSize - 1, lastOffset_, srcSize);
      lastOffset_ += srcSize;
    }

    collection_ = nullptr;
    hasNull_ = false;
    distincts.clear();
  }
};

std::shared_ptr<AggregateFunction> createFieldCollectAgg(
    const TypePtr& type,
    bool distinct = false) {
  auto typeKind = type->kind();
  switch (typeKind) {
    case TypeKind::ARRAY: {
      auto elementType = type->asArray().elementType();
      auto elementTypeKind = elementType->kind();
      switch (elementTypeKind) {
        case TypeKind::TINYINT:
          return std::make_shared<FieldCollectAgg<int8_t>>(distinct);
        case TypeKind::SMALLINT:
          return std::make_shared<FieldCollectAgg<int16_t>>(distinct);
        case TypeKind::INTEGER:
          return std::make_shared<FieldCollectAgg<int32_t>>(distinct);
        case TypeKind::BIGINT:
          return std::make_shared<FieldCollectAgg<int64_t>>(distinct);
        case TypeKind::REAL:
          return std::make_shared<FieldCollectAgg<float>>(distinct);
        case TypeKind::DOUBLE:
          return std::make_shared<FieldCollectAgg<double>>(distinct);
        default:
          BOLT_UNSUPPORTED(
              "Unsupported type for collect: ARRAY{}",
              mapTypeKindToName(elementTypeKind));
      }
    }
    default:
      BOLT_UNSUPPORTED(
          "Unsupported type for collect: {}", mapTypeKindToName(typeKind));
  }
}
} // namespace bytedance::bolt::connector::paimon
