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

#include "dwio/paimon/deletionvectors/DeletionVector.h"
namespace bytedance::bolt::paimon {

std::unique_ptr<DeletionVector> DeletionVector::create(
    const char* buf,
    int32_t length,
    bolt::memory::MemoryPool* pool) {
  return std::unique_ptr<DeletionVector>(
      new DeletionVector{roaring::Roaring::readSafe(buf, length), pool});
}

DeletionVector::DeletionVector(
    roaring::Roaring bitmap,
    bolt::memory::MemoryPool* pool)
    : roaring_(std::move(bitmap)), bitmapIt_(roaring_.begin()), pool_(pool) {}

void DeletionVector::getDeletionVector(
    uint32_t offset,
    uint32_t length,
    BufferPtr* buf) {
  prepareBuffer(buf, length);
  prepareIterator(offset);

  auto end = offset + length;
  auto data = (*buf)->asMutable<uint8_t>();
  while (bitmapIt_ != roaring_.end() && *bitmapIt_ < end) {
    bits::setBit(data, *bitmapIt_ - offset);
    lastValue_ = *bitmapIt_;
    ++bitmapIt_;
  }
}

void DeletionVector::prepareBuffer(BufferPtr* buf, uint32_t length) {
  BOLT_CHECK_NOT_NULL(buf);
  auto nbytes = bits::nbytes(length);
  if (!*buf) {
    *buf = AlignedBuffer::allocate<uint8_t>(nbytes, pool_, 0);
    return;
  }

  BOLT_CHECK((*buf)->isMutable());
  auto data = (*buf)->asMutable<uint8_t>();
  std::fill(data, data + (*buf)->size(), 0);
  if ((*buf)->size() < nbytes) {
    AlignedBuffer::reallocate<uint8_t>(buf, nbytes, 0);
  }
}

void DeletionVector::prepareIterator(uint32_t offset) {
  if (bitmapIt_ == roaring_.end()) {
    return;
  }
  if (*bitmapIt_ < offset || lastValue_ >= offset) {
    bitmapIt_.move_equalorlarger(offset);
  }
}
} // namespace bytedance::bolt::paimon
