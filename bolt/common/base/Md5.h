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

#include <openssl/md5.h>
#include <stddef.h>

#include <string>
namespace bytedance::bolt::common {

class Md5Digest {
 public:
  Md5Digest();

  void update(const void* data, size_t length);
  void finalize();
  // Write the 16-byte (binary) digest to the specified location
  size_t digest(char* out_digest);
  // Write the 32-character digest (in hexadecimal format) to the specified
  // location
  size_t digestHex(char* out_digest, bool upper = false);
  // Write the digest (in decimal format) to the specified location.
  // padding the string with 0 at beginning if the length is less than 32 and
  // the needPadding == true
  size_t digestDec(char* out_digest, bool padding = false);
  std::string digestHex(bool upper = false);
  std::string digestDec(bool padding = false);

 private:
  MD5_CTX md5Ctx_;
  unsigned char digest_[MD5_DIGEST_LENGTH];
  bool finalized_{false};
};
} // namespace bytedance::bolt::common