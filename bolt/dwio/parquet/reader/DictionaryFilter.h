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

#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/parquet/reader/ParquetTypeWithId.h"
#include "bolt/dwio/parquet/thrift/codegen/parquet_types.h"
namespace bytedance::bolt::common {
class Filter;
}
namespace bytedance::bolt::parquet {

/**
 * DictionaryFilter merges dictionary-based pruning logic:
 *  - checks if column is fully dictionary-encoded
 *  - determines the effective parquet type
 *  - decodes dictionary
 *  - tests each dictionary entry against Bolt filter
 */
class DictionaryFilter {
 public:
  DictionaryFilter(
      const thrift::ColumnChunk& columnChunk,
      const thrift::SchemaElement& schema,
      std::shared_ptr<const ParquetTypeWithId> type,
      dwio::common::BufferedInput& input,
      memory::MemoryPool& pool,
      const bolt::common::Filter* filter,
      uint32_t rowGroupIndex,
      size_t numOfRow)
      : columnChunk_(columnChunk), // Store whole ColumnChunk
        schema_(schema),
        type_(std::move(type)),
        input_(input),
        pool_(pool),
        filter_(filter),
        rowGroupIndex_(rowGroupIndex),
        numOfRow_(numOfRow) {}

  enum MatchResult { NoConclusion, Keep, Drop };
  MatchResult tryMatch();

 private:
  bool isDictionaryFriendly() const;

  template <typename T>
  MatchResult checkDictionaryValues();
  const thrift::ColumnChunk& columnChunk_;
  const thrift::SchemaElement& schema_;
  std::shared_ptr<const ParquetTypeWithId> type_;
  dwio::common::BufferedInput& input_;
  memory::MemoryPool& pool_;
  const bolt::common::Filter* filter_;
  uint32_t rowGroupIndex_;
  size_t numOfRow_;
};
} // namespace bytedance::bolt::parquet
