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

#include "bolt/dwio/parquet/reader/Decompression.h"

#include "bolt/common/compression/LzoDecompressor.h"
#include "bolt/dwio/common/BufferUtil.h"
#include "bolt/dwio/common/IntCodecCommon.h"
#include "bolt/dwio/common/SeekableInputStream.h"
#include "bolt/dwio/common/compression/Compression.h"
#include "bolt/dwio/parquet/reader/PageReader.h"
#include "bolt/dwio/parquet/reader/ParquetReaderUtil.h"
#include "bolt/dwio/parquet/thrift/FmtParquetFormatters.h"

#include <lz4.h>
#include <zstd.h>

namespace bytedance::bolt::parquet {

static inline std::uint32_t reverse_bytes(std::uint32_t i) {
  return (i & 0xff000000u) >> 24 | (i & 0x00ff0000u) >> 8 |
      (i & 0x0000ff00u) << 8 | (i & 0x000000ffu) << 24;
}

const char* FOLLY_NONNULL decompressLz4AndLzo(
    const char* compressedData,
    BufferPtr& decompressedData,
    uint32_t compressedSize,
    uint32_t uncompressedSize,
    memory::MemoryPool& pool,
    const thrift::CompressionCodec::type codec_) {
  dwio::common::ensureCapacity<char>(decompressedData, uncompressedSize, &pool);

  uint32_t decompressedTotalLength = 0;
  auto* inputPtr = compressedData;
  auto* outPtr = decompressedData->asMutable<char>();
  uint32_t inputLength = compressedSize;

  while (inputLength > 0) {
    if (inputLength < sizeof(uint32_t)) {
      BOLT_FAIL(
          "{} decompression failed, input len is to small: {}",
          codec_,
          inputLength)
    }
    uint32_t decompressedBlockLength =
        folly::Endian::big(folly::loadUnaligned<uint32_t>(inputPtr));
    inputPtr += dwio::common::INT_BYTE_SIZE;
    inputLength -= dwio::common::INT_BYTE_SIZE;
    uint32_t remainingOutputSize = uncompressedSize - decompressedTotalLength;
    if (remainingOutputSize < decompressedBlockLength) {
      BOLT_FAIL(
          "{} decompression failed, remainingOutputSize is less then "
          "decompressedBlockLength, remainingOutputSize: {}, "
          "decompressedBlockLength: {}",
          remainingOutputSize,
          decompressedBlockLength)
    }
    if (inputLength <= 0) {
      break;
    }

    do {
      // Check that input length should not be negative.
      if (inputLength < sizeof(uint32_t)) {
        BOLT_FAIL(
            "{} decompression failed, input len is to small: {}",
            codec_,
            inputLength)
      }
      // Read the length of the next lz4/lzo compressed block.
      uint32_t compressedLength =
          folly::Endian::big(folly::loadUnaligned<uint32_t>(inputPtr));
      inputPtr += dwio::common::INT_BYTE_SIZE;
      inputLength -= dwio::common::INT_BYTE_SIZE;

      if (compressedLength == 0) {
        continue;
      }

      if (compressedLength > inputLength) {
        BOLT_FAIL(
            "{} decompression failed, compressedLength is less then inputLength, "
            "compressedLength: {}, inputLength: {}",
            compressedLength,
            inputLength)
      }

      // Decompress this block.
      remainingOutputSize = uncompressedSize - decompressedTotalLength;
      uint64_t decompressedSize = -1;
      if (codec_ == thrift::CompressionCodec::LZ4) {
        decompressedSize = LZ4_decompress_safe(
            inputPtr,
            outPtr,
            static_cast<int32_t>(compressedLength),
            static_cast<int32_t>(remainingOutputSize));
      } else if (codec_ == thrift::CompressionCodec::LZO) {
        decompressedSize = common::compression::lzoDecompress(
            inputPtr,
            inputPtr + compressedLength,
            outPtr,
            outPtr + remainingOutputSize);
      } else {
        BOLT_FAIL("Unsupported Parquet compression type '{}'", codec_);
      }

      BOLT_CHECK_LE(decompressedSize, remainingOutputSize);

      outPtr += decompressedSize;
      inputPtr += compressedLength;
      inputLength -= compressedLength;
      decompressedBlockLength -= decompressedSize;
      decompressedTotalLength += decompressedSize;
    } while (decompressedBlockLength > 0);
  }

  BOLT_CHECK_EQ(decompressedTotalLength, uncompressedSize);

  return decompressedData->as<char>();
}

// Ref:https://parquet.apache.org/docs/file-format/data-pages/compression/
// Parquet file supports multiple compression codecs, including:
// Uncompressed, snappy, gzip, lzo, brotli, lz4, zstd, lz4_raw.
// Function 'thriftCodecToCompressionKind' will throw exception for brotli.
const char* FOLLY_NONNULL bdCodecDecompression(
    const char* pageData,
    BufferPtr& decompressedData,
    uint32_t compressedSize,
    uint32_t uncompressedSize,
    memory::MemoryPool& pool,
    const thrift::CompressionCodec::type codec) {
  std::unique_ptr<dwio::common::SeekableInputStream> inputStream =
      std::make_unique<dwio::common::SeekableArrayInputStream>(
          pageData, compressedSize, 0);
  auto streamDebugInfo =
      fmt::format("Page Reader: Stream {}", inputStream->getName());
  std::unique_ptr<dwio::common::SeekableInputStream> decompressedStream =
      dwio::common::compression::createDecompressor(
          thriftCodecToCompressionKind(codec),
          std::move(inputStream),
          uncompressedSize,
          pool,
          getParquetDecompressionOptions(thriftCodecToCompressionKind(codec)),
          streamDebugInfo,
          nullptr,
          true,
          compressedSize);

  decompressedStream->readFully(
      decompressedData->asMutable<char>(), uncompressedSize);

  return decompressedData->as<char>();
}

const char* FOLLY_NONNULL bdZstdDecompression(
    const char* pageData,
    BufferPtr& decompressedData,
    uint32_t compressedSize,
    uint32_t uncompressedSize,
    memory::MemoryPool& pool,
    const thrift::CompressionCodec::type codec) {
  BOLT_CHECK_GT(compressedSize, 4, "Not enough input bytes");
  int32_t zstd_type = *(int32_t*)(pageData);
  if (zstd_type == ZSTD_MAGICNUMBER) {
    auto ret = ZSTD_decompress(
        decompressedData->asMutable<char>(),
        uncompressedSize,
        pageData,
        compressedSize);
    DECOMPRESSION_ENSURE(
        !ZSTD_isError(ret),
        "ZSTD returned an error: {}",
        ZSTD_getErrorName(ret));
    BOLT_CHECK_EQ(ret, uncompressedSize);
  } else {
    uint32_t totalDecompressedCount = 0;
    uint32_t outputOffset = 0;
    uint32_t inputOffset = 0;
    uint32_t cumulativeUncompressedBlockLength = 0;
    auto output = decompressedData->asMutable<char>();

    while (totalDecompressedCount < uncompressedSize) {
      if (totalDecompressedCount == cumulativeUncompressedBlockLength) {
        cumulativeUncompressedBlockLength +=
            reverse_bytes(*(uint32_t*)(pageData + inputOffset));
        inputOffset += sizeof(uint32_t);
      }
      auto compressedChunkLength =
          reverse_bytes(*(uint32_t*)(pageData + inputOffset));
      inputOffset += sizeof(uint32_t);
      auto decompressionSize = ZSTD_decompress(
          output + outputOffset,
          uncompressedSize - outputOffset,
          pageData + inputOffset,
          compressedChunkLength);
      DECOMPRESSION_ENSURE(
          !ZSTD_isError(decompressionSize),
          "ZSTD returned an error: ",
          ZSTD_getErrorName(decompressionSize));
      totalDecompressedCount += decompressionSize;
      outputOffset += decompressionSize;
      inputOffset += compressedChunkLength;
    }
    BOLT_CHECK_EQ(outputOffset, uncompressedSize);
  }
  return decompressedData->as<char>();
}
} // namespace bytedance::bolt::parquet
