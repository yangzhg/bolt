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

#include "bolt/dwio/parquet/reader/DictionaryPageReader.h"
#include <common/base/Exceptions.h>
#include <dwio/common/MetricsLog.h>
#include <type/Type.h>
#include <memory>
namespace bytedance::bolt::parquet {

DictionaryPageReader::DictionaryPageReader(
    const thrift::ColumnChunk& columnChunk,
    std::shared_ptr<const ParquetTypeWithId> type,
    dwio::common::BufferedInput& input,
    memory::MemoryPool& pool,
    uint32_t rowGroupIndex)
    : columnChunk_(columnChunk), // Store the whole ColumnChunk
      meta_(columnChunk.meta_data),
      type_(std::move(type)),
      input_(input),
      pool_(pool),
      rowGroupIndex_(rowGroupIndex) {}

bool DictionaryPageReader::prepareDictionary() {
  // Check if dictionary page offset is valid
  if (!meta_.__isset.dictionary_page_offset ||
      meta_.dictionary_page_offset <= 0) {
    return false;
  }

  // Calculate dictionary page size:
  size_t dictionaryRegionSize =
      meta_.data_page_offset - meta_.dictionary_page_offset;

  // Create an input stream for the dictionary page region
  auto seekableInput = input_.read(
      meta_.dictionary_page_offset,
      dictionaryRegionSize,
      dwio::common::LogType::HEADER);

  // Create the page reader
  auto pageReader = std::make_unique<PageReader>(
      std::move(seekableInput),
      pool_,
      type_,
      meta_.codec,
      dictionaryRegionSize,
      nullptr);

  // Read page header and verify it is a dictionary page
  pageHeader_ = pageReader->readPageHeader();

  // Verify this is actually a dictionary page
  if (pageHeader_.type != thrift::PageType::DICTIONARY_PAGE) {
    return false;
  }

  // The PageHeader contains compressed_page_size which tells us the exact size
  // of the dictionary data. We've already read the header, so the reader is
  // now positioned at the start of the compressed dictionary data.
  reader_ = std::move(pageReader);
  return true;
}

const thrift::PageHeader DictionaryPageReader::pageHeader() const {
  return pageHeader_;
}

template <typename T>
bool DictionaryPageReader::readDictionaryValues(VectorPtr& outValues) {
  // Check if dictionary page offset is valid
  if (reader_ == nullptr) {
    return false;
  }
  if (pageHeader_.type != thrift::PageType::DICTIONARY_PAGE ||
      !pageHeader_.__isset.dictionary_page_header) {
    return false;
  }

  reader_->prepareDictionary(pageHeader_);
  auto dictionaryValues = reader_->dictionaryValues(getTypeForTemplate<T>());
  if (!dictionaryValues) {
    return false;
  }

  outValues = std::move(dictionaryValues);
  return true;
}

template <typename T>
TypePtr DictionaryPageReader::getTypeForTemplate() {
  if constexpr (
      std::is_same_v<T, StringView> || std::is_same_v<T, std::string>) {
    return VARCHAR();
  } else if constexpr (std::is_same_v<T, int128_t>) {
    return HUGEINT();
  } else if constexpr (std::is_same_v<T, int64_t>) {
    return BIGINT();
  } else if constexpr (std::is_same_v<T, int32_t>) {
    return INTEGER();
  } else if constexpr (std::is_same_v<T, int16_t>) {
    return SMALLINT();
  } else if constexpr (std::is_same_v<T, int8_t>) {
    return TINYINT();
  } else if constexpr (std::is_same_v<T, Timestamp>) {
    return TIMESTAMP();
  } else if constexpr (std::is_same_v<T, bool>) {
    return BOOLEAN();
  } else if constexpr (std::is_same_v<T, float>) {
    return REAL();
  } else if constexpr (std::is_same_v<T, double>) {
    return DOUBLE();
  } else {
    BOLT_UNREACHABLE("Unsupported type for dictionary values");
  }
}

// Explicit template instantiations for supported types
template bool DictionaryPageReader::readDictionaryValues<StringView>(
    VectorPtr&);
template bool DictionaryPageReader::readDictionaryValues<std::string>(
    VectorPtr&);
template bool DictionaryPageReader::readDictionaryValues<int128_t>(VectorPtr&);
template bool DictionaryPageReader::readDictionaryValues<int64_t>(VectorPtr&);
template bool DictionaryPageReader::readDictionaryValues<int32_t>(VectorPtr&);
template bool DictionaryPageReader::readDictionaryValues<int16_t>(VectorPtr&);
template bool DictionaryPageReader::readDictionaryValues<int8_t>(VectorPtr&);
template bool DictionaryPageReader::readDictionaryValues<Timestamp>(VectorPtr&);
template bool DictionaryPageReader::readDictionaryValues<bool>(VectorPtr&);
template bool DictionaryPageReader::readDictionaryValues<float>(VectorPtr&);
template bool DictionaryPageReader::readDictionaryValues<double>(VectorPtr&);

// Explicit template instantiations for getTypeForTemplate
template TypePtr DictionaryPageReader::getTypeForTemplate<StringView>();
template TypePtr DictionaryPageReader::getTypeForTemplate<std::string>();
template TypePtr DictionaryPageReader::getTypeForTemplate<int128_t>();
template TypePtr DictionaryPageReader::getTypeForTemplate<int64_t>();
template TypePtr DictionaryPageReader::getTypeForTemplate<int32_t>();
template TypePtr DictionaryPageReader::getTypeForTemplate<int16_t>();
template TypePtr DictionaryPageReader::getTypeForTemplate<int8_t>();
template TypePtr DictionaryPageReader::getTypeForTemplate<Timestamp>();
template TypePtr DictionaryPageReader::getTypeForTemplate<bool>();
template TypePtr DictionaryPageReader::getTypeForTemplate<float>();
template TypePtr DictionaryPageReader::getTypeForTemplate<double>();

} // namespace bytedance::bolt::parquet