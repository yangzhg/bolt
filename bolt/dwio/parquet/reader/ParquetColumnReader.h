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

#include "bolt/dwio/parquet/reader/ParquetData.h"

namespace bytedance::bolt::dwio::common {
struct ColumnReaderOptions;
} // namespace bytedance::bolt::dwio::common

namespace bytedance::bolt::parquet {

/// Parquet stores integers of 8, 16, 32 bits with 32 bits and 64 as 64 bits.
inline int32_t parquetSizeOfIntKind(TypeKind kind) {
  switch (kind) {
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
      return 4;
    case TypeKind::BIGINT:
      return 8;
    case TypeKind::HUGEINT:
      return 16;
    default:
      BOLT_FAIL("Not an integer TypeKind");
  }
}

/// Wrapper for static functions for Parquet columns.
class ParquetColumnReader {
 public:
  /// Builds a reader tree producing 'fileType'. The metadata is in 'params'.
  /// The filters and pruning are in 'scanSpec'.
  static std::unique_ptr<dwio::common::SelectiveColumnReader> build(
      const dwio::common::ColumnReaderOptions& columnReaderOptions,
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      ParquetParams& params,
      common::ScanSpec& scanSpec,
      memory::MemoryPool& pool);
};
} // namespace bytedance::bolt::parquet
