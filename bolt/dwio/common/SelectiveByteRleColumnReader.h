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

class SelectiveByteRleColumnReader : public SelectiveColumnReader {
 public:
  SelectiveByteRleColumnReader(
      const TypePtr& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      dwio::common::FormatParams& params,
      bolt::common::ScanSpec& scanSpec)
      : SelectiveColumnReader(
            requestedType,
            std::move(fileType),
            params,
            scanSpec) {}

  bool hasBulkPath() const override {
    return false;
  }

  void getValues(const RowSet& rows, VectorPtr* result) override;

  template <typename Reader, bool isDense, typename ExtractValues>
  void processFilter(
      bolt::common::Filter* filter,
      ExtractValues extractValues,
      const RowSet& rows);

  template <typename Reader, bool isDense>
  void processValueHook(const RowSet& rows, ValueHook* hook);

  template <
      typename Reader,
      typename TFilter,
      bool isDense,
      typename ExtractValues>
  void readHelper(
      bolt::common::Filter* filter,
      RowSet rows,
      ExtractValues extractValues);

  template <typename Reader>
  void
  readCommon(int64_t offset, const RowSet& rows, const uint64_t* incomingNulls);
};

template <
    typename Reader,
    typename TFilter,
    bool isDense,
    typename ExtractValues>
void SelectiveByteRleColumnReader::readHelper(
    bolt::common::Filter* filter,
    RowSet rows,
    ExtractValues extractValues) {
  reinterpret_cast<Reader*>(this)->readWithVisitor(
      rows,
      ColumnVisitor<int8_t, TFilter, ExtractValues, isDense>(
          *reinterpret_cast<TFilter*>(filter), this, rows, extractValues));
}

template <typename Reader, bool isDense, typename ExtractValues>
void SelectiveByteRleColumnReader::processFilter(
    bolt::common::Filter* filter,
    ExtractValues extractValues,
    const RowSet& rows) {
  using bolt::common::FilterKind;
  switch (filter ? filter->kind() : FilterKind::kAlwaysTrue) {
    case FilterKind::kAlwaysTrue:
      readHelper<Reader, bolt::common::AlwaysTrue, isDense>(
          filter, rows, extractValues);
      break;
    case FilterKind::kIsNull:
      filterNulls<int8_t>(
          rows,
          true,
          !std::is_same_v<decltype(extractValues), dwio::common::DropValues>);
      break;
    case FilterKind::kIsNotNull:
      if (std::is_same_v<decltype(extractValues), dwio::common::DropValues>) {
        filterNulls<int8_t>(rows, false, false);
      } else {
        readHelper<Reader, bolt::common::IsNotNull, isDense>(
            filter, rows, extractValues);
      }
      break;
    case FilterKind::kBigintRange:
      readHelper<Reader, bolt::common::BigintRange, isDense>(
          filter, rows, extractValues);
      break;
    case FilterKind::kNegatedBigintRange:
      readHelper<Reader, bolt::common::NegatedBigintRange, isDense>(
          filter, rows, extractValues);
      break;
    case FilterKind::kBigintValuesUsingBitmask:
      readHelper<Reader, bolt::common::BigintValuesUsingBitmask, isDense>(
          filter, rows, extractValues);
      break;
    case FilterKind::kNegatedBigintValuesUsingBitmask:
      readHelper<
          Reader,
          bolt::common::NegatedBigintValuesUsingBitmask,
          isDense>(filter, rows, extractValues);
      break;
    default:
      readHelper<Reader, bolt::common::Filter, isDense>(
          filter, rows, extractValues);
      break;
  }
}

template <typename Reader, bool isDense>
void SelectiveByteRleColumnReader::processValueHook(
    const RowSet& rows,
    ValueHook* hook) {
  using namespace bytedance::bolt::aggregate;
  switch (hook->kind()) {
    case aggregate::AggregationHook::kSumBigintToBigint:
      readHelper<Reader, bolt::common::AlwaysTrue, isDense>(
          &dwio::common::alwaysTrue(),
          rows,
          dwio::common::ExtractToHook<SumHook<int64_t, int64_t>>(hook));
      break;
    default:
      readHelper<Reader, bolt::common::AlwaysTrue, isDense>(
          &dwio::common::alwaysTrue(),
          rows,
          dwio::common::ExtractToGenericHook(hook));
  }
}

template <typename Reader>
void SelectiveByteRleColumnReader::readCommon(
    int64_t offset,
    const RowSet& rows,
    const uint64_t* incomingNulls) {
  prepareRead<int8_t>(offset, rows, incomingNulls);
  const bool isDense = rows.back() == rows.size() - 1;
  bolt::common::Filter* filter =
      scanSpec_->filter() ? scanSpec_->filter() : &dwio::common::alwaysTrue();
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        processValueHook<Reader, true>(rows, scanSpec_->valueHook());
      } else {
        processValueHook<Reader, false>(rows, scanSpec_->valueHook());
      }
      return;
    }
    if (isDense) {
      processFilter<Reader, true>(
          filter, dwio::common::ExtractToReader(this), rows);
    } else {
      processFilter<Reader, false>(
          filter, dwio::common::ExtractToReader(this), rows);
    }
  } else {
    if (isDense) {
      processFilter<Reader, true>(filter, dwio::common::DropValues(), rows);
    } else {
      processFilter<Reader, false>(filter, dwio::common::DropValues(), rows);
    }
  }
}

} // namespace bytedance::bolt::dwio::common
