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

#pragma once

#include "bolt/dwio/common/SelectiveColumnReaderInternal.h"
namespace bytedance::bolt::dwio::common {

// Abstract class for format and encoding-independent parts of reading ingeger
// columns.
class SelectiveIntegerColumnReader : public SelectiveColumnReader {
 public:
  SelectiveIntegerColumnReader(
      const TypePtr& requestedType,
      dwio::common::FormatParams& params,
      bolt::common::ScanSpec& scanSpec,
      std::shared_ptr<const dwio::common::TypeWithId> fileType)
      : SelectiveColumnReader(
            requestedType,
            std::move(fileType),
            params,
            scanSpec) {}

  void getValues(const RowSet& rows, VectorPtr* result) override {
    getIntValues(rows, requestedType_, result);
  }

 protected:
  // Switches based on filter type between different readHelper instantiations.
  template <typename Reader, bool isDense, typename ExtractValues>
  void processFilter(
      bolt::common::Filter* filter,
      ExtractValues extractValues,
      const RowSet& rows);

  // Switches based on the type of ValueHook between different readWithVisitor
  // instantiations.
  template <typename Reader, bool isDence>
  void processValueHook(const RowSet& rows, ValueHook* hook);

  // Instantiates a Visitor based on type, isDense, value processing.
  template <
      typename Reader,
      typename TFilter,
      bool isDense,
      typename ExtractValues>
  void readHelper(
      bolt::common::Filter* filter,
      const RowSet& rows,
      ExtractValues extractValues);

  // The common part of integer reading. calls the appropriate
  // instantiation of processValueHook or processFilter based on
  // possible value hook, filter and denseness.
  template <typename Reader>
  void readCommon(const RowSet& rows);
};

template <
    typename Reader,
    typename TFilter,
    bool isDense,
    typename ExtractValues>
void SelectiveIntegerColumnReader::readHelper(
    bolt::common::Filter* filter,
    const RowSet& rows,
    ExtractValues extractValues) {
  switch (valueSize_) {
    case 2:
      reinterpret_cast<Reader*>(this)->Reader::readWithVisitor(
          rows,
          ColumnVisitor<int16_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
      break;

    case 4:
      reinterpret_cast<Reader*>(this)->Reader::readWithVisitor(
          rows,
          ColumnVisitor<int32_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
      break;

    case 8:
      reinterpret_cast<Reader*>(this)->Reader::readWithVisitor(
          rows,
          ColumnVisitor<int64_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
      break;

    case 16:
      reinterpret_cast<Reader*>(this)->Reader::readWithVisitor(
          rows,
          ColumnVisitor<int128_t, TFilter, ExtractValues, isDense>(
              *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
      break;

    default:
      BOLT_FAIL("Unsupported valueSize_ {}", valueSize_);
  }
}

template <typename Reader, bool isDense, typename ExtractValues>
void SelectiveIntegerColumnReader::processFilter(
    bolt::common::Filter* filter,
    ExtractValues extractValues,
    const RowSet& rows) {
  switch (filter ? filter->kind() : bolt::common::FilterKind::kAlwaysTrue) {
    case bolt::common::FilterKind::kAlwaysTrue:
      static_cast<Reader*>(this)
          ->template readHelper<Reader, bolt::common::AlwaysTrue, isDense>(
              &dwio::common::alwaysTrue(), rows, extractValues);
      break;
    case bolt::common::FilterKind::kIsNull:
      static_cast<Reader*>(this)->template filterNulls<int64_t>(
          rows, true, !std::is_same_v<decltype(extractValues), DropValues>);
      break;
    case bolt::common::FilterKind::kIsNotNull:
      if (std::is_same_v<decltype(extractValues), DropValues>) {
        static_cast<Reader*>(this)->template filterNulls<int64_t>(
            rows, false, false);
      } else {
        static_cast<Reader*>(this)
            ->template readHelper<Reader, bolt::common::IsNotNull, isDense>(
                filter, rows, extractValues);
      }
      break;
    case bolt::common::FilterKind::kBigintRange:
      static_cast<Reader*>(this)
          ->template readHelper<Reader, bolt::common::BigintRange, isDense>(
              filter, rows, extractValues);
      break;
    case bolt::common::FilterKind::kNegatedBigintRange:
      static_cast<Reader*>(this)
          ->template readHelper<
              Reader,
              bolt::common::NegatedBigintRange,
              isDense>(filter, rows, extractValues);
      break;
    case bolt::common::FilterKind::kBigintValuesUsingHashTable:
      static_cast<Reader*>(this)
          ->template readHelper<
              Reader,
              bolt::common::BigintValuesUsingHashTable,
              isDense>(filter, rows, extractValues);
      break;
    case bolt::common::FilterKind::kBigintValuesUsingBitmask:
      static_cast<Reader*>(this)
          ->template readHelper<
              Reader,
              bolt::common::BigintValuesUsingBitmask,
              isDense>(filter, rows, extractValues);
      break;
    case bolt::common::FilterKind::kNegatedBigintValuesUsingHashTable:
      static_cast<Reader*>(this)
          ->template readHelper<
              Reader,
              bolt::common::NegatedBigintValuesUsingHashTable,
              isDense>(filter, rows, extractValues);
      break;
    case bolt::common::FilterKind::kNegatedBigintValuesUsingBitmask:
      static_cast<Reader*>(this)
          ->template readHelper<
              Reader,
              bolt::common::NegatedBigintValuesUsingBitmask,
              isDense>(filter, rows, extractValues);
      break;
    default:
      static_cast<Reader*>(this)
          ->template readHelper<Reader, bolt::common::Filter, isDense>(
              filter, rows, extractValues);
      break;
  }
}

template <typename Reader, bool isDense>
void SelectiveIntegerColumnReader::processValueHook(
    const RowSet& rows,
    ValueHook* hook) {
  switch (hook->kind()) {
    case aggregate::AggregationHook::kSumBigintToBigint:
      readHelper<Reader, bolt::common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::SumHook<int64_t, int64_t, false>>(hook));
      break;
    case aggregate::AggregationHook::kSumBigintToBigintOverflow:
      readHelper<Reader, bolt::common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::SumHook<int64_t, int64_t, true>>(hook));
      break;
    case aggregate::AggregationHook::kBigintMax:
      readHelper<Reader, bolt::common::AlwaysTrue, isDense>(
          &dwio::common::alwaysTrue(),
          rows,
          ExtractToHook<aggregate::MinMaxHook<int64_t, false>>(hook));
      break;
    case aggregate::AggregationHook::kBigintMin:
      readHelper<Reader, bolt::common::AlwaysTrue, isDense>(
          &alwaysTrue(),
          rows,
          ExtractToHook<aggregate::MinMaxHook<int64_t, true>>(hook));
      break;
    default:
      readHelper<Reader, bolt::common::AlwaysTrue, isDense>(
          &alwaysTrue(), rows, ExtractToGenericHook(hook));
  }
}

template <typename Reader>
void SelectiveIntegerColumnReader::readCommon(const RowSet& rows) {
  bool isDense = rows.back() == rows.size() - 1;
  bolt::common::Filter* filter =
      scanSpec_->filter() ? scanSpec_->filter() : &alwaysTrue();
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        processValueHook<Reader, true>(rows, scanSpec_->valueHook());
      } else {
        processValueHook<Reader, false>(rows, scanSpec_->valueHook());
      }
    } else {
      if (isDense) {
        processFilter<Reader, true>(filter, ExtractToReader(this), rows);
      } else {
        processFilter<Reader, false>(filter, ExtractToReader(this), rows);
      }
    }
  } else {
    if (isDense) {
      processFilter<Reader, true>(filter, DropValues(), rows);
    } else {
      processFilter<Reader, false>(filter, DropValues(), rows);
    }
  }
}

} // namespace bytedance::bolt::dwio::common
