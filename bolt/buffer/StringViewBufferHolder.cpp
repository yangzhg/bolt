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

#include "bolt/buffer/StringViewBufferHolder.h"
#include "bolt/buffer/Buffer.h"
#include "bolt/type/StringView.h"
namespace bytedance::bolt {

StringView StringViewBufferHolder::getOwnedStringView(
    const char* data,
    int32_t size) {
  if (stringBuffers_.empty() ||
      stringBuffers_.back()->size() + size >
          stringBuffers_.back()->capacity()) {
    stringBuffers_.push_back(AlignedBuffer::allocate<char>(
        std::max(size, kInitialStringReservation), pool_));
    stringBuffers_.back()->setSize(0);
  }
  auto stringBuffer = stringBuffers_.back().get();
  char* newPtr = stringBuffer->asMutable<char>() + stringBuffer->size();
  memcpy(newPtr, data, size);
  stringBuffer->setSize(stringBuffer->size() + size);
  return StringView(newPtr, size);
}

StringView StringViewBufferHolder::getOwnedStringView(StringView stringView) {
  if (stringView.isInline()) {
    return stringView;
  }
  return getOwnedStringView(stringView.data(), stringView.size());
}

} // namespace bytedance::bolt
