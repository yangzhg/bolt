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

#include "bolt/dwio/common/SelectiveIntegerColumnReader.h"
#include "bolt/dwio/dwrf/common/DecoderUtil.h"
#include "bolt/dwio/dwrf/reader/DwrfData.h"
namespace bytedance::bolt::dwrf {

class SelectiveIntegerDictionaryColumnReader
    : public dwio::common::SelectiveIntegerColumnReader {
 public:
  using ValueType = int64_t;

  SelectiveIntegerDictionaryColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      std::shared_ptr<const dwio::common::TypeWithId> fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec,
      uint32_t numBytes);

  void seekToRowGroup(int64_t index) override {
    SelectiveIntegerColumnReader::seekToRowGroup(index);
    auto positionsProvider = formatData_->seekToRowGroup(index);
    if (inDictionaryReader_) {
      inDictionaryReader_->seekToRowGroup(positionsProvider);
    }

    dataReader_->seekToRowGroup(positionsProvider);

    BOLT_CHECK(!positionsProvider.hasNext());
  }

  uint64_t skip(uint64_t numValues) override;

  void read(int64_t offset, const RowSet& rows, const uint64_t* incomingNulls)
      override;

  template <typename ColumnVisitor>
  void readWithVisitor(const RowSet& rows, ColumnVisitor visitor);

 private:
  void ensureInitialized();

  std::unique_ptr<ByteRleDecoder> inDictionaryReader_;
  std::unique_ptr<dwio::common::IntDecoder</* isSigned = */ false>> dataReader_;
  std::unique_ptr<dwio::common::IntDecoder</* isSigned = */ true>> dictReader_;
  std::function<BufferPtr()> dictInit_;
  RleVersion rleVersion_;
  bool initialized_{false};
};

template <typename ColumnVisitor>
void SelectiveIntegerDictionaryColumnReader::readWithVisitor(
    const RowSet& rows,
    ColumnVisitor visitor) {
  auto dictVisitor = visitor.toDictionaryColumnVisitor();
  if (rleVersion_ == RleVersion_1) {
    decodeWithVisitor<bolt::dwrf::RleDecoderV1<false>>(
        dataReader_.get(), dictVisitor);
  } else {
    decodeWithVisitor<bolt::dwrf::RleDecoderV2<false>>(
        dataReader_.get(), dictVisitor);
  }
}
} // namespace bytedance::bolt::dwrf
