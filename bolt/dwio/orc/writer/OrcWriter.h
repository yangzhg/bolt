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

#include <arrow/record_batch.h>
#include "bolt/common/compression/Compression.h"

#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/common/FlushPolicy.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/Writer.h"
#include "bolt/dwio/common/WriterFactory.h"
#include "bolt/vector/arrow/Bridge.h"
namespace bytedance::bolt::orc::writer {

class ArrowDataBufferSink;

struct ArrowOrcContext;

class DefaultFlushPolicy : public dwio::common::FlushPolicy {
 public:
  DefaultFlushPolicy()
      : rowsInRowGroup_(1'024 * 1'024), bytesInRowGroup_(128 * 1'024 * 1'024) {}
  DefaultFlushPolicy(uint64_t rowsInRowGroup, int64_t bytesInRowGroup)
      : rowsInRowGroup_(rowsInRowGroup), bytesInRowGroup_(bytesInRowGroup) {}

  bool shouldFlush(
      const dwio::common::StripeProgress& stripeProgress) override {
    return stripeProgress.stripeRowCount >= rowsInRowGroup_ ||
        stripeProgress.stripeSizeEstimate >= bytesInRowGroup_;
  }

  void onClose() override {
    // No-op
  }

  uint64_t rowsInRowGroup() const {
    return rowsInRowGroup_;
  }

  int64_t bytesInRowGroup() const {
    return bytesInRowGroup_;
  }

  virtual void setBytesPerRow(int64_t bytes) {}

  virtual void setMemLimit(int64_t memLimit) {}

 private:
  const uint64_t rowsInRowGroup_;
  const int64_t bytesInRowGroup_;
};

class LambdaFlushPolicy : public DefaultFlushPolicy {
 public:
  explicit LambdaFlushPolicy(
      uint64_t rowsInRowGroup,
      int64_t bytesInRowGroup,
      std::function<bool()> lambda)
      : DefaultFlushPolicy(rowsInRowGroup, bytesInRowGroup) {
    lambda_ = std::move(lambda);
  }
  virtual ~LambdaFlushPolicy() override = default;

  bool shouldFlush(
      const dwio::common::StripeProgress& stripeProgress) override {
    return lambda_() || DefaultFlushPolicy::shouldFlush(stripeProgress);
  }

 private:
  std::function<bool()> lambda_;
};

class SizeFlushPolicy : public DefaultFlushPolicy {
 public:
  int64_t KDefaultSizeMultiplier = 2;

  explicit SizeFlushPolicy(int64_t bytesInRowGroup)
      : DefaultFlushPolicy(
            std::numeric_limits<int64_t>::max(),
            bytesInRowGroup) {}

  virtual ~SizeFlushPolicy() override = default;

  bool shouldFlush(
      const dwio::common::StripeProgress& stripeProgress) override {
    if (memLimit_ > 0 && stripeProgress.stripeSizeEstimate >= memLimit_) {
      return true;
    } else if (bytesPerRowCompressed_ > 0) {
      return stripeProgress.stripeRowCount * bytesPerRowCompressed_ >=
          bytesInRowGroup();
    } else {
      return stripeProgress.stripeSizeEstimate >=
          bytesInRowGroup() * KDefaultSizeMultiplier;
    }
  }

  void setBytesPerRow(int64_t bytes) override {
    bytesPerRowCompressed_ = bytes;
  }

  void setMemLimit(int64_t memLimit) override {
    memLimit_ = std::max(memLimit, bytesInRowGroup());
  }

 private:
  int64_t bytesPerRowCompressed_{0};
  int64_t memLimit_{0};
};

struct BoltOrcWriteOption {
  bool enableDictionary = true;
  bool enableRowGroupAlignedWrite = false;
  bool enableFlushBasedOnBlockSize = false;
  int64_t parquet_block_size = -1;
  std::vector<int32_t> expectedRowsInEachBlock;
  int64_t dataPageSize = 1'024 * 1'024;
  int64_t dictionaryPageSizeLimit = 1'024 * 1'024;
  // Growth ratio passed to ArrowDataBufferSink. The default value is a
  // heuristic borrowed from
  // folly/FBVector(https://github.com/facebook/folly/blob/main/folly/docs/FBVector.md#memory-handling).
  double bufferGrowRatio = 1.5;
  double bufferReserveRatio = 0; // 0 denotes we don't pre-reserve buffer
  common::CompressionKind compression = common::CompressionKind_NONE;
  bolt::memory::MemoryPool* memoryPool;
  /// Timestamp unit for Parquet write through Arrow bridge.
  /// Default if not specified: TimestampUnit::kNano (9).
  std::optional<TimestampUnit> orcWriteTimestampUnit;
  std::optional<std::string> orcWriteTimestampTimeZone;
  bool writeInt96AsTimestamp = false;
};

// Writes Bolt vectors into  a DataSink using OCRFileWriter.
class ArrowOrcWriter : public dwio::common::Writer {
 public:
  // Constructs a writer with output to 'sink'. A new row group is
  // started every 'rowsInRowGroup' top level rows. 'pool' is used for
  // temporary memory. 'properties' specifies Parquet-specific
  // options. 'schema' specifies the file's overall schema, and it is always
  // non-null.
  ArrowOrcWriter(
      std::unique_ptr<dwio::common::FileSink> sink,
      const BoltOrcWriteOption& options,
      std::shared_ptr<memory::MemoryPool> pool,
      arrow::MemoryPool* arrowPool,
      RowTypePtr schema);

  ArrowOrcWriter(
      std::unique_ptr<dwio::common::FileSink> sink,
      const BoltOrcWriteOption& options,
      RowTypePtr schema);

  ~ArrowOrcWriter() override = default;

  static bool isCodecAvailable(common::CompressionKind compression);

  // Appends 'data' into the writer.
  void write(const VectorPtr& data) override;

  void write(const VectorPtr& data, int64_t memLimit);

  void flush() override;
  void flush(int64_t rowsInCurrentRowGroup);

  // Closes 'this', After close, data can no longer be added and the completed
  // Parquet file is flushed into 'sink' provided at construction. 'sink' stays
  // live until destruction of 'this'.
  void close() override;

  void abort() override;

  // Used in data retention scenario. Write parquet with specific row numbers in
  // each row group.
  void rowGroupAlignedFlush(
      int32_t numRows,
      uint64_t bytes,
      std::shared_ptr<::arrow::RecordBatch>& recordBatch);

 private:
  // Sets the memory reclaimers for all the memory pools used by this writer.
  void setMemoryReclaimers();

  void writeRecordBatch(std::shared_ptr<::arrow::RecordBatch>& recordBatch);

  // Pool for 'stream_'.
  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<memory::MemoryPool> generalPool_;
  arrow::MemoryPool* arrowPool_{nullptr};

  // Temporary Arrow stream for capturing the output.
  std::shared_ptr<ArrowDataBufferSink> stream_;
  std::shared_ptr<ArrowOrcContext> arrowContext_;
  std::unique_ptr<DefaultFlushPolicy> flushPolicy_;

  const RowTypePtr schema_;

  ArrowOptions options_{.flattenDictionary = true, .flattenConstant = true};
  const double bufferReserveRatio_;
  const bool enableRowGroupAlignedWrite_;
  const std::vector<int32_t> expectedRowsInEachBlock_;
  const bool enableFlushBasedOnBlockSize_;

  int32_t currentRowGroup_ = 0;
};

class OrcWriterFactory : public dwio::common::WriterFactory {
 public:
  OrcWriterFactory() : WriterFactory(dwio::common::FileFormat::ORC) {}

  std::unique_ptr<dwio::common::Writer> createWriter(
      std::unique_ptr<dwio::common::FileSink> sink,
      const dwio::common::WriterOptions& options) override;
};

} // namespace bytedance::bolt::orc::writer
