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

#include "bolt/dwio/common/DirectDecoder.h"
#include "bolt/common/base/BitUtil.h"
#include "bolt/dwio/common/SeekableInputStream.h"
namespace bytedance::bolt::dwio::common {

template <bool isSigned>
void DirectDecoder<isSigned>::seekToRowGroup(
    dwio::common::PositionProvider& location) {
  // move the input stream
  IntDecoder<isSigned>::inputStream->seekToPosition(location);
  // force a re-read from the stream
  IntDecoder<isSigned>::bufferEnd = IntDecoder<isSigned>::bufferStart;
  this->pendingSkip = 0;
}

template void DirectDecoder<true>::seekToRowGroup(
    dwio::common::PositionProvider& location);
template void DirectDecoder<false>::seekToRowGroup(
    dwio::common::PositionProvider& location);

template <bool isSigned>
template <typename T>
void DirectDecoder<isSigned>::nextValues(
    T* data,
    uint64_t numValues,
    const uint64_t* nulls) {
  skipPending();
  uint64_t position = 0;
  // skipNulls()
  if (nulls) {
    // Skip over null values.
    while (position < numValues && bits::isBitNull(nulls, position)) {
      ++position;
    }
  }

  // this is gross and very not DRY, but helps avoid branching
  if (position < numValues) {
    if (nulls) {
      if (!IntDecoder<isSigned>::useVInts) {
        if constexpr (std::is_same_v<T, int128_t>) {
          BOLT_NYI();
        }
        for (uint64_t i = position; i < numValues; ++i) {
          if (!bits::isBitNull(nulls, i)) {
            data[i] = IntDecoder<isSigned>::readLongLE();
          }
        }
      } else {
        for (uint64_t i = position; i < numValues; ++i) {
          if (!bits::isBitNull(nulls, i)) {
            data[i] = IntDecoder<isSigned>::template readVInt<T>();
          }
        }
      }
    } else {
      if (!IntDecoder<isSigned>::useVInts) {
        if constexpr (std::is_same_v<T, int128_t>) {
          BOLT_NYI();
        }
        for (uint64_t i = position; i < numValues; ++i) {
          data[i] = IntDecoder<isSigned>::readLongLE();
        }
      } else {
        for (uint64_t i = position; i < numValues; ++i) {
          data[i] = IntDecoder<isSigned>::template readVInt<T>();
        }
      }
    }
  }
}

template void DirectDecoder<true>::nextValues(
    int64_t* data,
    uint64_t numValues,
    const uint64_t* nulls);

template void DirectDecoder<true>::nextValues(
    int128_t* data,
    uint64_t numValues,
    const uint64_t* nulls);

template void DirectDecoder<false>::nextValues(
    int64_t* data,
    uint64_t numValues,
    const uint64_t* nulls);

template void DirectDecoder<false>::nextValues(
    int128_t* data,
    uint64_t numValues,
    const uint64_t* nulls);

} // namespace bytedance::bolt::dwio::common
