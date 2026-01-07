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

#include "bolt/dwio/parquet/reader/ParquetReaderCli.h"
namespace bytedance::bolt::parquet {

void ParquetReaderCli::prepareParquetReaderCli() {
  dwio::common::RowReaderOptions rowReaderOpts;
  rowReaderOpts.select(
      std::make_shared<bytedance::bolt::dwio::common::ColumnSelector>(
          rowType_, rowType_->names()));
  rowReaderOpts.setScanSpec(scanSpec_);
  rowReader_ = reader_->createRowReader(rowReaderOpts);
}

uint64_t ParquetReaderCli::read(std::shared_ptr<BaseVector>& result) {
  return read(result, readBatchSize_);
}

uint64_t ParquetReaderCli::read(
    std::shared_ptr<BaseVector>& result,
    uint64_t batchSize) {
  auto numRowsRead = rowReader_->next(batchSize, result);
  if (numRowsRead == 0) {
    return 0;
  }

  auto rowVector = result->asUnchecked<RowVector>();
  for (auto i = 0; i < rowVector->childrenSize(); ++i) {
    rowVector->childAt(i)->loadedVector();
  }

  return numRowsRead;
}

uint64_t ParquetReaderCli::skip(uint64_t skipSize) {
  uint64_t totalSkipped = 0;
  while (skipSize > 0) {
    auto skipped = rowReader_->skip(skipSize);
    if (skipped == 0) {
      break;
    }
    totalSkipped += skipped;
    skipSize -= skipped;
  }
  return totalSkipped;
}
} // namespace bytedance::bolt::parquet
