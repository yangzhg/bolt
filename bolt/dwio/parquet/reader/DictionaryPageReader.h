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

#include <vector>
#include "bolt/common/memory/Memory.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/parquet/reader/PageReader.h"
#include "bolt/dwio/parquet/thrift/codegen/parquet_types.h"
#include "bolt/type/StringView.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt::parquet {

/**
 * DictionaryPageReader:
 *  - Locates, reads, and decompresses a Parquet dictionary page.
 *  - Provides templated decode function to parse dictionary entries
 *    into a typed vector (e.g. INT64 or StringView).
 */
class DictionaryPageReader {
 public:
  DictionaryPageReader(
      const thrift::ColumnChunk& columnChunk,
      std::shared_ptr<const ParquetTypeWithId> type,
      dwio::common::BufferedInput& input,
      memory::MemoryPool& pool,
      uint32_t rowGroupIndex);

  template <typename T>
  bool readDictionaryValues(VectorPtr& outValues);
  bool prepareDictionary();
  const thrift::PageHeader pageHeader() const;

 private:
  template <typename T>
  TypePtr getTypeForTemplate();

  const thrift::ColumnChunk& columnChunk_;
  const thrift::ColumnMetaData& meta_;
  std::shared_ptr<const ParquetTypeWithId> type_;
  dwio::common::BufferedInput& input_;
  memory::MemoryPool& pool_;
  uint32_t rowGroupIndex_;

  thrift::PageHeader pageHeader_;
  std::unique_ptr<PageReader> reader_;
};

// Template specializations declarations for supported types
extern template bool DictionaryPageReader::readDictionaryValues<StringView>(
    VectorPtr&);
extern template bool DictionaryPageReader::readDictionaryValues<std::string>(
    VectorPtr&);
extern template bool DictionaryPageReader::readDictionaryValues<int128_t>(
    VectorPtr&);
extern template bool DictionaryPageReader::readDictionaryValues<int64_t>(
    VectorPtr&);
extern template bool DictionaryPageReader::readDictionaryValues<int32_t>(
    VectorPtr&);
extern template bool DictionaryPageReader::readDictionaryValues<int16_t>(
    VectorPtr&);
extern template bool DictionaryPageReader::readDictionaryValues<int8_t>(
    VectorPtr&);
extern template bool DictionaryPageReader::readDictionaryValues<Timestamp>(
    VectorPtr&);
extern template bool DictionaryPageReader::readDictionaryValues<bool>(
    VectorPtr&);
extern template bool DictionaryPageReader::readDictionaryValues<float>(
    VectorPtr&);
extern template bool DictionaryPageReader::readDictionaryValues<double>(
    VectorPtr&);

} // namespace bytedance::bolt::parquet