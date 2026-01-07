/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

#include <arrow/util/compression.h>
namespace bytedance::bolt::shuffle::sparksql {

enum CodecBackend { NONE, QAT, IAA };

// BUFFER mode will preallocate max compressed buffer, and then compress each
// buffer to the max compressed buffer ROWVECTOR mode will copy the buffers to a
// big buffer and then compress the big buffer
enum CompressionMode { BUFFER, ROWVECTOR };

inline CodecBackend getCodecBackend(std::string codecBackend) {
  if (codecBackend == "none") {
    return CodecBackend::NONE;
  } else if (codecBackend == "qat") {
    return CodecBackend::QAT;
  } else if (codecBackend == "iaa") {
    return CodecBackend::IAA;
  } else {
    throw std::invalid_argument(
        "Not support this codec backend " + codecBackend);
  }
}

inline CompressionMode getCompressionMode(std::string compressionMode) {
  if (compressionMode == "buffer") {
    return CompressionMode::BUFFER;
  } else if (compressionMode == "rowvector") {
    return CompressionMode::ROWVECTOR;
  } else {
    throw std::invalid_argument(
        "Not support this compression mode " + compressionMode);
  }
}

std::unique_ptr<arrow::util::Codec> createArrowIpcCodec(
    arrow::Compression::type compressedType,
    CodecBackend codecBackend,
    int32_t compressionLevel = arrow::util::kUseDefaultCompressionLevel);

} // namespace bytedance::bolt::shuffle::sparksql
