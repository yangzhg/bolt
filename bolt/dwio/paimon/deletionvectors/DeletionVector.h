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

#include "bolt/buffer/Buffer.h"
#include "roaring/roaring.hh"
namespace bytedance::bolt::paimon {
class DeletionVector {
 public:
  static std::unique_ptr<DeletionVector>
  create(const char* buf, int32_t length, bolt::memory::MemoryPool* pool);

  void getDeletionVector(uint32_t offset, uint32_t length, BufferPtr* buf);

  uint32_t getLastValue() const {
    return lastValue_;
  }

 protected:
  DeletionVector(roaring::Roaring bitmap, bolt::memory::MemoryPool* pool);

 private:
  void prepareBuffer(BufferPtr* buf, uint32_t length);
  void prepareIterator(uint32_t offset);

  const roaring::Roaring roaring_;
  roaring::Roaring::const_iterator bitmapIt_;
  uint32_t lastValue_{0};
  bolt::memory::MemoryPool* pool_;
};
} // namespace bytedance::bolt::paimon