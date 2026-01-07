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

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/file/File.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/dwio/common/ScanSpec.h"
#include "bolt/dwio/common/tests/utils/DataSetBuilder.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/dwio/parquet/tests/ParquetTestBase.h"
#include "bolt/type/Filter.h"

#include "bolt/common/file/FileSystems.h"
#include "bolt/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#include "bolt/core/Config.h"
namespace bytedance::bolt::parquet {

enum class FileLocation { LOCAL, HDFS };

// This ParquetReaderCli can be used to read Parquet files directly.
class ParquetReaderCli {
 public:
  ParquetReaderCli(
      uint32_t readBatchSize,
      FileLocation fileLocation,
      std::string path,
      std::vector<std::string> columnNames,
      memory::MemoryPool* leafPool)
      : readBatchSize_(readBatchSize),
        fileLocation_(fileLocation),
        leafPool_(leafPool) {
    file_ = loadFile(path);
    dwio::common::ReaderOptions readerOpts{leafPool_};
    auto input = std::make_unique<dwio::common::BufferedInput>(
        file_, readerOpts.getMemoryPool());

    reader_ = std::make_unique<ParquetReader>(std::move(input), readerOpts);

    defaultRowType_ = reader_->fileSchema();

    if (columnNames.empty()) {
      rowType_ = defaultRowType_;
    } else {
      std::vector<TypePtr> types;
      for (auto& name : columnNames) {
        types.push_back(
            defaultRowType_->childAt(defaultRowType_->getChildIdx(name)));
      }
      rowType_ = ROW(std::move(columnNames), std::move(types));
    }

    scanSpec_ = std::make_shared<common::ScanSpec>("root");
    scanSpec_->addAllChildFields(*rowType_);
    prepareParquetReaderCli();
  }

  ParquetReaderCli(
      uint32_t readBatchSize,
      FileLocation fileLocation,
      std::string path,
      std::vector<std::string> columnNames,
      int rowGroupIdx,
      memory::MemoryPool* leafPool)
      : readBatchSize_(readBatchSize),
        fileLocation_(fileLocation),
        leafPool_(leafPool) {
    file_ = loadFile(path);
    dwio::common::ReaderOptions readerOpts{leafPool_};
    auto input = std::make_unique<dwio::common::BufferedInput>(
        file_, readerOpts.getMemoryPool());

    reader_ = std::make_unique<ParquetReader>(std::move(input), readerOpts);

    defaultRowType_ = reader_->fileSchema();

    if (columnNames.empty()) {
      rowType_ = defaultRowType_;
    } else {
      std::vector<TypePtr> types;
      for (auto& name : columnNames) {
        types.push_back(
            defaultRowType_->childAt(defaultRowType_->getChildIdx(name)));
      }
      rowType_ = ROW(std::move(columnNames), std::move(types));
    }

    scanSpec_ = std::make_shared<common::ScanSpec>("root");
    scanSpec_->addAllChildFields(*rowType_);
    loadRowGroup(rowGroupIdx);
  }

  explicit ParquetReaderCli(
      uint32_t readBatchSize,
      memory::MemoryPool* rootPool,
      memory::MemoryPool* leafPool,
      FileLocation fileLocation,
      std::shared_ptr<ReadFile> file,
      std::shared_ptr<const RowType> rowType,
      std::shared_ptr<common::ScanSpec> scanSpec)
      : readBatchSize_(readBatchSize),
        fileLocation_(fileLocation),
        file_(file),
        rootPool_(rootPool),
        leafPool_(leafPool),
        rowType_(rowType),
        scanSpec_(scanSpec) {
    dwio::common::ReaderOptions readerOpts{leafPool_};
    auto input = std::make_unique<dwio::common::BufferedInput>(
        file_, readerOpts.getMemoryPool());

    reader_ = std::make_unique<ParquetReader>(std::move(input), readerOpts);

    prepareParquetReaderCli();
  }

  VectorPtr prepareResult() {
    return BaseVector::create(rowType_, 0, leafPool_);
  }

  int64_t getFileRowNumbers() {
    return reader_->fileMetaData().numRows();
  }

  int getFileRowGroupNumbers() {
    return reader_->fileMetaData().numRowGroups();
  }

  int getRowGroupSize(int rowGroupIdx) {
    return reader_->fileMetaData().rowGroup(rowGroupIdx).numRows();
  }

  int getRowGroupRowNumbers(int rowGroupIdx) {
    return reader_->fileMetaData().rowGroup(rowGroupIdx).totalCompressedSize();
  }

  int64_t getRowGroupFileOffset(int rowGroupIdx) {
    return reader_->fileMetaData().rowGroup(rowGroupIdx).fileOffset();
  }

  const std::shared_ptr<const RowType> fileSchema() const {
    return defaultRowType_;
  }

  void loadRowGroup(int rowGroupIdx) {
    dwio::common::RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(
        std::make_shared<bytedance::bolt::dwio::common::ColumnSelector>(
            rowType_, rowType_->names()));
    rowReaderOpts.setScanSpec(scanSpec_);
    auto fileOffset = getRowGroupFileOffset(rowGroupIdx);
    rowReaderOpts.range(fileOffset, fileOffset + 1);
    rowReader_ = reader_->createRowReader(rowReaderOpts);
  }

  /**
   * Read at most readBatchSize_ rows from the file.
   * @param result Vector to collect the results.
   * @return number of rows that have been read in this round. If filters
   * exist, the number of rows in the result vector might be smaller because
   * some rows might be discarted after applying the filters. It returns
   * none-zero values if there are remaining data in the file. Otherwise, it
   * returns 0 when the file is exhausted.
   */
  uint64_t read(std::shared_ptr<BaseVector>& result);

  uint64_t read(std::shared_ptr<BaseVector>& result, uint64_t batchSize);

  /**
   * Skip at most skipSize rows from the file.
   * @param skipSize number of rows to skip.
   * @return number of rows that have been skipped. This return value might be
   * smaller than skipSize when the whole file is exhausted in this round of
   * skip.
   */
  uint64_t skip(uint64_t skipSize);

 private:
  void prepareParquetReaderCli();

  std::shared_ptr<ReadFile> loadFile(std::string& path) {
    std::shared_ptr<ReadFile> file;
    switch (fileLocation_) {
      case FileLocation::LOCAL:
        file = std::make_shared<LocalReadFile>(path);
        break;
      case FileLocation::HDFS:
        filesystems::registerHdfsFileSystem();
        auto memConfig = std::make_shared<config::ConfigBase>(
            std::unordered_map<std::string, std::string>());
        auto hdfsFileSystem = filesystems::getFileSystem(path, memConfig);
        file = hdfsFileSystem->openFileForRead(path);
        break;
    }

    return file;
  }

  uint32_t readBatchSize_;
  FileLocation fileLocation_;
  std::shared_ptr<ReadFile> file_;
  memory::MemoryPool* rootPool_;
  memory::MemoryPool* leafPool_;
  std::shared_ptr<const RowType> defaultRowType_;
  std::shared_ptr<const RowType> rowType_;
  std::shared_ptr<common::ScanSpec> scanSpec_;
  std::unique_ptr<dwio::common::RowReader> rowReader_;
  std::shared_ptr<ParquetReader> reader_;
};

} // namespace bytedance::bolt::parquet
