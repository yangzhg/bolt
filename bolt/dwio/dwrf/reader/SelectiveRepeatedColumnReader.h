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

#include "bolt/dwio/common/BufferUtil.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/SelectiveColumnReaderInternal.h"
#include "bolt/dwio/common/SelectiveRepeatedColumnReader.h"
#include "bolt/dwio/dwrf/common/DecoderUtil.h"
#include "bolt/dwio/dwrf/reader/DwrfData.h"
namespace bytedance::bolt::dwrf {

class SelectiveListColumnReader
    : public dwio::common::SelectiveListColumnReader {
 public:
  SelectiveListColumnReader(
      const dwio::common::ColumnReaderOptions& columnReaderOptions,
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec);

  void resetFilterCaches() override {
    child_->resetFilterCaches();
  }

  void seekToRowGroup(int64_t index) override {
    dwio::common::SelectiveListColumnReader::seekToRowGroup(index);
    auto positionsProvider = formatData_->seekToRowGroup(index);
    length_->seekToRowGroup(positionsProvider);

    BOLT_CHECK(!positionsProvider.hasNext());

    child_->seekToRowGroup(index);
    child_->setReadOffsetRecursive(0);
    childTargetReadOffset_ = 0;
  }

  void readLengths(
      int32_t* FOLLY_NONNULL lengths,
      int32_t numLengths,
      const uint64_t* FOLLY_NULLABLE nulls) override {
    length_->next(lengths, numLengths, nulls);
  }

 private:
  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>> length_;
};

class SelectiveMapColumnReader : public dwio::common::SelectiveMapColumnReader {
 public:
  SelectiveMapColumnReader(
      const dwio::common::ColumnReaderOptions& columnReaderOptions,
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec);

  void resetFilterCaches() override {
    keyReader_->resetFilterCaches();
    elementReader_->resetFilterCaches();
  }

  void seekToRowGroup(int64_t index) override {
    dwio::common::SelectiveMapColumnReader::seekToRowGroup(index);
    auto positionsProvider = formatData_->seekToRowGroup(index);

    length_->seekToRowGroup(positionsProvider);

    BOLT_CHECK(!positionsProvider.hasNext());

    keyReader_->seekToRowGroup(index);
    keyReader_->setReadOffsetRecursive(0);
    elementReader_->seekToRowGroup(index);
    elementReader_->setReadOffsetRecursive(0);
    childTargetReadOffset_ = 0;
  }

  void readLengths(
      int32_t* FOLLY_NONNULL lengths,
      int32_t numLengths,
      const uint64_t* FOLLY_NULLABLE nulls) override {
    length_->next(lengths, numLengths, nulls);
  }

 private:
  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>> length_;
};

} // namespace bytedance::bolt::dwrf
