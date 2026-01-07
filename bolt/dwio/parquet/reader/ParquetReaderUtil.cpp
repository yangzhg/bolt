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

#include "bolt/dwio/parquet/reader/ParquetReaderUtil.h"
namespace bytedance::bolt::parquet {

common::CompressionKind thriftCodecToCompressionKind(
    thrift::CompressionCodec::type codec) {
  switch (codec) {
    case thrift::CompressionCodec::UNCOMPRESSED:
      return common::CompressionKind::CompressionKind_NONE;
      break;
    case thrift::CompressionCodec::SNAPPY:
      return common::CompressionKind::CompressionKind_SNAPPY;
      break;
    case thrift::CompressionCodec::GZIP:
      return common::CompressionKind::CompressionKind_GZIP;
      break;
    case thrift::CompressionCodec::LZO:
      return common::CompressionKind::CompressionKind_LZO;
      break;
    case thrift::CompressionCodec::LZ4:
      return common::CompressionKind::CompressionKind_LZ4;
      break;
    case thrift::CompressionCodec::ZSTD:
      return common::CompressionKind::CompressionKind_ZSTD;
      break;
    case thrift::CompressionCodec::LZ4_RAW:
      return common::CompressionKind::CompressionKind_LZ4;
    default:
      BOLT_UNSUPPORTED(
          "Unsupported compression type: " +
          bytedance::bolt::parquet::thrift::to_string(codec));
      break;
  }
}
} // namespace bytedance::bolt::parquet
