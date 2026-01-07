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

#include <gtest/gtest.h>

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/compression/Compression.h"
namespace bytedance::bolt::common {

class CompressionTest : public testing::Test {};

TEST_F(CompressionTest, testCompressionNames) {
  EXPECT_EQ("none", compressionKindToString(CompressionKind_NONE));
  EXPECT_EQ("zlib", compressionKindToString(CompressionKind_ZLIB));
  EXPECT_EQ("snappy", compressionKindToString(CompressionKind_SNAPPY));
  EXPECT_EQ("lzo", compressionKindToString(CompressionKind_LZO));
  EXPECT_EQ("lz4", compressionKindToString(CompressionKind_LZ4));
  EXPECT_EQ("zstd", compressionKindToString(CompressionKind_ZSTD));
  EXPECT_EQ(
      "unknown - 99",
      compressionKindToString(static_cast<CompressionKind>(99)));
}

TEST_F(CompressionTest, compressionKindToCodec) {
  ASSERT_EQ(
      folly::io::CodecType::NO_COMPRESSION,
      compressionKindToCodec(CompressionKind::CompressionKind_NONE)->type());
  ASSERT_EQ(
      folly::io::CodecType::ZLIB,
      compressionKindToCodec(CompressionKind::CompressionKind_ZLIB)->type());
  ASSERT_EQ(
      folly::io::CodecType::SNAPPY,
      compressionKindToCodec(CompressionKind::CompressionKind_SNAPPY)->type());
  ASSERT_EQ(
      folly::io::CodecType::ZSTD,
      compressionKindToCodec(CompressionKind::CompressionKind_ZSTD)->type());
  ASSERT_EQ(
      folly::io::CodecType::LZ4,
      compressionKindToCodec(CompressionKind::CompressionKind_LZ4)->type());
  ASSERT_EQ(
      folly::io::CodecType::GZIP,
      compressionKindToCodec(CompressionKind::CompressionKind_GZIP)->type());
  EXPECT_THROW(
      compressionKindToCodec(CompressionKind::CompressionKind_LZO),
      bytedance::bolt::BoltException);
}

TEST_F(CompressionTest, codecToCompressionKind) {
  ASSERT_EQ(
      CompressionKind::CompressionKind_NONE,
      codecTypeToCompressionKind(folly::io::CodecType::NO_COMPRESSION));
  ASSERT_EQ(
      CompressionKind::CompressionKind_ZLIB,
      codecTypeToCompressionKind(folly::io::CodecType::ZLIB));
  ASSERT_EQ(
      CompressionKind::CompressionKind_SNAPPY,
      codecTypeToCompressionKind(folly::io::CodecType::SNAPPY));
  ASSERT_EQ(
      CompressionKind::CompressionKind_ZSTD,
      codecTypeToCompressionKind(folly::io::CodecType::ZSTD));
  ASSERT_EQ(
      CompressionKind::CompressionKind_LZ4,
      codecTypeToCompressionKind(folly::io::CodecType::LZ4));
  ASSERT_EQ(
      CompressionKind::CompressionKind_GZIP,
      codecTypeToCompressionKind(folly::io::CodecType::GZIP));
  EXPECT_THROW(
      codecTypeToCompressionKind(folly::io::CodecType::ZSTD_FAST),
      bytedance::bolt::BoltException);
}

TEST_F(CompressionTest, stringToCompressionKind) {
  EXPECT_EQ(stringToCompressionKind("none"), CompressionKind_NONE);
  EXPECT_EQ(stringToCompressionKind("zlib"), CompressionKind_ZLIB);
  EXPECT_EQ(stringToCompressionKind("snappy"), CompressionKind_SNAPPY);
  EXPECT_EQ(stringToCompressionKind("lzo"), CompressionKind_LZO);
  EXPECT_EQ(stringToCompressionKind("lz4"), CompressionKind_LZ4);
  EXPECT_EQ(stringToCompressionKind("zstd"), CompressionKind_ZSTD);
  EXPECT_EQ(stringToCompressionKind("gzip"), CompressionKind_GZIP);
  BOLT_ASSERT_THROW(
      stringToCompressionKind("bz2"), "Not support compression kind bz2");
}
} // namespace bytedance::bolt::common
