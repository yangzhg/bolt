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
namespace bytedance::bolt::paimon {

class DeletionVector;

// read record <size of serialized bin, serialized bin, checksum of serialized
// bin>
class DeletionFileReader {
 public:
  struct Options {
    // offset of record in deletion file
    int64_t offset{0};
    // size of serialized bin
    int64_t size{0};
    bolt::memory::MemoryPool* memoryPool{nullptr};
  };

  DeletionFileReader(
      std::unique_ptr<dwio::common::BufferedInput> input,
      Options options);

  ~DeletionFileReader();

  void getDeletionVector(uint32_t offset, uint32_t length, BufferPtr* buf);

 private:
  void initDeletionVector();

  std::unique_ptr<dwio::common::BufferedInput> input_;
  Options options_;
  std::unique_ptr<DeletionVector> deletionVector_;
};
} // namespace bytedance::bolt::paimon