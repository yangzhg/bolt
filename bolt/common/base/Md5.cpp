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

#include "bolt/common/base/Md5.h"

#include <folly/Conv.h>
#include <folly/Format.h>
#include <cstring>
namespace bytedance::bolt::common {
Md5Digest::Md5Digest() {
  MD5_Init(&md5Ctx_);
}

void Md5Digest::update(const void* data, size_t length) {
  MD5_Update(&md5Ctx_, data, length);
}

void Md5Digest::finalize() {
  if (!finalized_) {
    MD5_Final(digest_, &md5Ctx_);
    finalized_ = true;
  }
}

size_t Md5Digest::digest(char* out_digest) {
  if (!finalized_) {
    finalize();
  }
  memcpy(out_digest, digest_, MD5_DIGEST_LENGTH);
  return MD5_DIGEST_LENGTH;
}

size_t Md5Digest::digestHex(char* out_digest, bool upper) {
  if (!finalized_) {
    finalize();
  }
  static char digVecLower[] = "0123456789abcdef";
  static char digVecUpper[] = "0123456789ABCDEF";
  char* digVec = upper ? digVecUpper : digVecLower;
  for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
    *out_digest++ = digVec[digest_[i] >> 4];
    *out_digest++ = digVec[digest_[i] & 0x0F];
  }
  return 2 * MD5_DIGEST_LENGTH;
}

size_t Md5Digest::digestDec(char* out_digest, bool padding) {
  if (!finalized_) {
    finalize();
  }

  auto dec = padding ? folly::sformat("{:0>*}", 32, digestDec()) : digestDec();
  int size = dec.size();
  std::memcpy(out_digest, dec.data(), size);
  return size;
}

std::string Md5Digest::digestHex(bool upper) {
  std::string hex;
  hex.resize(2 * MD5_DIGEST_LENGTH);
  digestHex(hex.data(), upper);
  return hex;
}

std::string Md5Digest::digestDec(bool padding) {
  if (!finalized_) {
    finalize();
  }
  __uint128_t val = 0;
  for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
    val = static_cast<__uint128_t>(val << 4) | ((digest_[i] >> 4) & 0xf);
    val = static_cast<__uint128_t>(val << 4) | (digest_[i] & 0xf);
  }
  auto dec = folly::to<std::string>(static_cast<__uint128_t>(val));
  return dec;
}
} // namespace bytedance::bolt::common
