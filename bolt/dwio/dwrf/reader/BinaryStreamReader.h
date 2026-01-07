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

#include "bolt/dwio/common/ColumnSelector.h"
#include "bolt/dwio/dwrf/common/Common.h"
#include "bolt/dwio/dwrf/reader/StripeReaderBase.h"
#include "bolt/dwio/dwrf/reader/StripeStream.h"
namespace bytedance::bolt::dwrf::detail {

class BinaryStripeStreams {
 public:
  BinaryStripeStreams(
      StripeReaderBase& stripeReader,
      const dwio::common::ColumnSelector& selector,
      uint32_t stripeIndex);

  std::vector<proto::ColumnEncoding> getEncodings(uint32_t nodeId) const;

  std::vector<DwrfStreamIdentifier> getStreamIdentifiers(uint32_t nodeId) const;

  std::unique_ptr<proto::RowIndex> getRowGroupIndex(
      const EncodingKey ek,
      std::string_view label) const {
    return ProtoUtils::readProto<proto::RowIndex>(stripeStreams_.getStream(
        ek.forKind(proto::Stream_Kind_ROW_INDEX), label, false));
  }

  std::unique_ptr<dwio::common::SeekableInputStream> getStream(
      const DwrfStreamIdentifier& si,
      std::string_view label) const {
    return stripeStreams_.getCompressedStream(si, label);
  }

  uint64_t getStreamLength(const DwrfStreamIdentifier& si) const {
    return stripeStreams_.getStreamLength(si);
  }

  const StripeInformationWrapper& getStripeInfo() const {
    return stripeInfo_;
  }

 private:
  bool preload_;
  StripeInformationWrapper stripeInfo_;
  dwio::common::RowReaderOptions options_;
  StripeStreamsImpl stripeStreams_;
  folly::F14FastMap<uint32_t, std::vector<uint32_t>> encodingKeys_;
  folly::F14FastMap<uint32_t, std::vector<DwrfStreamIdentifier>>
      nodeToStreamIdMap_;
};

class BinaryStreamReader {
 public:
  explicit BinaryStreamReader(
      const std::shared_ptr<ReaderBase>& reader,
      const std::vector<uint64_t>& columnIds);

  uint32_t getStrideLen() const;

  std::unique_ptr<BinaryStripeStreams> next();

  std::unordered_map<uint32_t, proto::ColumnStatistics> getStatistics() const;

  uint32_t getCurrentStripeIndex() const {
    return stripeIndex_;
  }

 private:
  StripeReaderBase stripeReaderBase_;
  const dwio::common::ColumnSelector columnSelector_;
  uint32_t stripeIndex_;

 public:
  const uint32_t numStripes;
};

} // namespace bytedance::bolt::dwrf::detail
