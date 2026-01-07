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

#include "bolt/common/memory/Memory.h"
#include "bolt/dwio/common/ColumnSelector.h"
#include "bolt/dwio/common/FormatData.h"
#include "bolt/dwio/common/TypeWithId.h"
#include "bolt/dwio/common/compression/Compression.h"
#include "bolt/dwio/dwrf/common/ByteRLE.h"
#include "bolt/dwio/dwrf/common/RLEv1.h"
#include "bolt/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "bolt/dwio/dwrf/reader/EncodingContext.h"
#include "bolt/dwio/dwrf/reader/StripeStream.h"
#include "bolt/vector/BaseVector.h"
namespace bytedance::bolt::dwrf {

// DWRF specific functions shared between all readers.
class DwrfData : public dwio::common::FormatData {
 public:
  DwrfData(
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      StripeStreams& stripe,
      const StreamLabels& streamLabels,
      FlatMapContext flatMapContext);

  void readNulls(
      vector_size_t numValues,
      const uint64_t* FOLLY_NULLABLE incomingNulls,
      BufferPtr& nulls,
      bool nullsOnly = false) override;

  uint64_t skipNulls(uint64_t numValues, bool nullsOnly = false) override;

  uint64_t skip(uint64_t numValues) override {
    return skipNulls(numValues);
  }

  void filterRowGroups(
      const common::ScanSpec& scanSpec,
      uint64_t rowsPerRowGroup,
      const dwio::common::StatsContext& writerContext,
      FilterRowGroupsResult&,
      dwio::common::BufferedInput& /*input*/) override;

  bool hasNulls() const override {
    return notNullDecoder_ != nullptr;
  }

  auto* FOLLY_NULLABLE notNullDecoder() const {
    return notNullDecoder_.get();
  }

  const FlatMapContext& flatMapContext() {
    return flatMapContext_;
  }

  const uint64_t* FOLLY_NULLABLE inMap() const {
    return flatMapContext_.inMapDecoder ? inMap_->as<uint64_t>() : nullptr;
  }

  /// Seeks possible flat map in map streams and nulls to the row group
  /// and returns a PositionsProvider for the other streams.
  dwio::common::PositionProvider seekToRowGroup(int64_t index) override;

  int64_t stripeRows() const {
    return stripeRows_;
  }

  std::optional<int64_t> rowsPerRowGroup() const override {
    return rowsPerRowGroup_;
  }

  // Decodes the entry from row group index for 'this' in the stripe,
  // if not already decoded. Throws if no index.
  void ensureRowGroupIndex();

  auto& index() const {
    return *index_;
  }

 private:
  static std::vector<uint64_t> toPositionsInner(
      const proto::RowIndexEntry& entry) {
    return std::vector<uint64_t>(
        entry.positions().begin(), entry.positions().end());
  }

  memory::MemoryPool& memoryPool_;
  const std::shared_ptr<const dwio::common::TypeWithId> fileType_;
  FlatMapContext flatMapContext_;
  std::unique_ptr<ByteRleDecoder> notNullDecoder_;
  std::unique_ptr<dwio::common::SeekableInputStream> indexStream_;
  std::unique_ptr<proto::RowIndex> index_;
  int64_t stripeRows_;
  // Number of rows in a row group. Last row group may have fewer rows.
  uint32_t rowsPerRowGroup_;

  // Storage for positions backing a PositionProvider returned from
  // seekToRowGroup().
  std::vector<uint64_t> tempPositions_;

  // In map bitmap used in flat map encoding.
  BufferPtr inMap_;
};

// DWRF specific initialization.
class DwrfParams : public dwio::common::FormatParams {
 public:
  explicit DwrfParams(
      StripeStreams& stripeStreams,
      const StreamLabels& streamLabels,
      dwio::common::ColumnReaderStatistics& stats,
      FlatMapContext context = {})
      : FormatParams(stripeStreams.getMemoryPool(), stats),
        stripeStreams_(stripeStreams),
        flatMapContext_(context),
        streamLabels_(streamLabels) {}

  std::unique_ptr<dwio::common::FormatData> toFormatData(
      const std::shared_ptr<const dwio::common::TypeWithId>& type,
      const common::ScanSpec& /*scanSpec*/) override {
    return std::make_unique<DwrfData>(
        type, stripeStreams_, streamLabels_, flatMapContext_);
  }

  StripeStreams& stripeStreams() {
    return stripeStreams_;
  }

  FlatMapContext flatMapContext() const {
    return flatMapContext_;
  }

  const StreamLabels& streamLabels() const {
    return streamLabels_;
  }

 private:
  StripeStreams& stripeStreams_;
  FlatMapContext flatMapContext_;
  const StreamLabels& streamLabels_;
};

inline RleVersion convertRleVersion(proto::ColumnEncoding_Kind kind) {
  switch (static_cast<int64_t>(kind)) {
    case proto::ColumnEncoding_Kind_DIRECT:
    case proto::ColumnEncoding_Kind_DICTIONARY:
      return RleVersion_1;
    case proto::ColumnEncoding_Kind_DIRECT_V2:
    case proto::ColumnEncoding_Kind_DICTIONARY_V2:
      return RleVersion_2;
    default:
      DWIO_RAISE("Unknown encoding in convertRleVersion");
  }
}

} // namespace bytedance::bolt::dwrf
