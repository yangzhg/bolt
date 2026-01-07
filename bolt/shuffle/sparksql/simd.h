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
#include <stdint.h>
namespace bytedance::bolt::shuffle::sparksql {

inline uint8_t extractBitsToByte(const uint8_t* srcAddr, uint32_t* offset) {
  uint8_t dst = 0;
  uint8_t src = 0;
  auto srcOffset = offset[0]; /*16k*/
  src = srcAddr[srcOffset >> 3];
  // PREFETCHT0((&(srcAddr)[(srcOffset >> 3) + 64]));
  dst = src >> (srcOffset & 7) |
      0xfe; // get the bit in bit 0, other bits set to 1

  srcOffset = offset[1]; /*16k*/
  src = srcAddr[srcOffset >> 3];
  dst &= src >> (srcOffset & 7) << 1 |
      0xfd; // get the bit in bit 0, other bits set to 1

  srcOffset = offset[2]; /*16k*/
  src = srcAddr[srcOffset >> 3];
  dst &= src >> (srcOffset & 7) << 2 |
      0xfb; // get the bit in bit 0, other bits set to 1

  srcOffset = offset[3]; /*16k*/
  src = srcAddr[srcOffset >> 3];
  dst &= src >> (srcOffset & 7) << 3 |
      0xf7; // get the bit in bit 0, other bits set to 1

  srcOffset = offset[4]; /*16k*/
  src = srcAddr[srcOffset >> 3];
  dst &= src >> (srcOffset & 7) << 4 |
      0xef; // get the bit in bit 0, other bits set to 1

  srcOffset = offset[5]; /*16k*/
  src = srcAddr[srcOffset >> 3];
  dst &= src >> (srcOffset & 7) << 5 |
      0xdf; // get the bit in bit 0, other bits set to 1

  srcOffset = offset[6]; /*16k*/
  src = srcAddr[srcOffset >> 3];
  dst &= src >> (srcOffset & 7) << 6 |
      0xbf; // get the bit in bit 0, other bits set to 1

  srcOffset = offset[7]; /*16k*/
  src = srcAddr[srcOffset >> 3];
  dst &= src >> (srcOffset & 7) << 7 |
      0x7f; // get the bit in bit 0, other bits set to 1

  return dst;
}

inline uint8_t extractBitsToByteSimd(const uint8_t* srcAddr, uint32_t* offset) {
#if defined(__x86_64__)
  // load 8xi16 offset and extend to 8xi32
  __m256i offsetVec = _mm256_loadu_si256((__m256i*)offset);
  // gather 8xi32 from offset
  __m256i srcNullVec = _mm256_i32gather_epi32(
      (const int*)srcAddr, _mm256_srli_epi32(offsetVec, 5), 4);
  // get bit offset within i32
  __m256i offsetIn4ByteVec = _mm256_and_si256(
      offsetVec,
      _mm256_set_epi32(0x1F, 0x1F, 0x1F, 0x1F, 0x1F, 0x1F, 0x1F, 0x1F));
  // get bit in lowest bit of 8xi32
  __m256i bitVec = _mm256_srav_epi32(srcNullVec, offsetIn4ByteVec);
  // shift bit to the most significant bit of 8xi32
  __m256i bitInSignVec = _mm256_slli_epi32(bitVec, 31);
  // convert 8xi32 into 8xf32 and extract the most significant bit
  uint8_t result =
      (uint8_t)_mm256_movemask_ps(_mm256_cvtepi32_ps(bitInSignVec));
  return result;
#else
  return extractBitsToByte(srcAddr, offset);
#endif
}

} // namespace bytedance::bolt::shuffle::sparksql
