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

#include "bolt/common/encode/Base64.h"

#include <gtest/gtest.h>
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/tests/GTestUtils.h"

namespace bytedance::bolt::encoding {

class Base64Test : public ::testing::Test {};

TEST_F(Base64Test, calculateMimeDecodedSize) {
  EXPECT_EQ(0, Base64::calculateMimeDecodedSize("", 0));
  EXPECT_EQ(0, Base64::calculateMimeDecodedSize("#", 1));
  EXPECT_EQ(3, Base64::calculateMimeDecodedSize("TWFu", 4));
  EXPECT_EQ(1, Base64::calculateMimeDecodedSize("AQ==", 4));
  EXPECT_EQ(2, Base64::calculateMimeDecodedSize("TWE=", 4));
  EXPECT_EQ(3, Base64::calculateMimeDecodedSize("TWFu\r\n", 6));
  EXPECT_EQ(3, Base64::calculateMimeDecodedSize("!TW!Fu!", 7));
  EXPECT_EQ(1, Base64::calculateMimeDecodedSize("TQ", 2));
  BOLT_ASSERT_USER_THROW(
      Base64::calculateMimeDecodedSize("A", 1),
      "Input should at least have 2 bytes for base64 bytes.");
}

TEST_F(Base64Test, decodeMime) {
  auto decodeMime = [](const std::string& in) {
    size_t decSize = Base64::calculateMimeDecodedSize(in.data(), in.size());
    std::string out(decSize, '\0');
    Base64::decodeMime(in.data(), in.size(), out.data());
    return out;
  };
  EXPECT_EQ("", decodeMime(""));
  EXPECT_EQ("Man", decodeMime("TWFu"));
  EXPECT_EQ("ManMan", decodeMime("TWFu\r\nTWFu"));
  EXPECT_EQ("\x01", decodeMime("AQ=="));
  EXPECT_EQ("\xff\xee", decodeMime("/+4="));
  BOLT_ASSERT_USER_THROW(
      decodeMime("QUFBx"), "Last unit does not have enough valid bits");
  BOLT_ASSERT_USER_THROW(
      decodeMime("xx=y"), "Input byte array has wrong 4-byte ending unit");
  BOLT_ASSERT_USER_THROW(
      decodeMime("xx="), "Input byte array has wrong 4-byte ending unit");
  BOLT_ASSERT_USER_THROW(
      decodeMime("QUFB="), "Input byte array has wrong 4-byte ending unit");
  BOLT_ASSERT_USER_THROW(
      decodeMime("AQ==y"), "Input byte array has incorrect ending");
}

TEST_F(Base64Test, calculateMimeEncodedSize) {
  EXPECT_EQ(0, Base64::calculateMimeEncodedSize(0));
  EXPECT_EQ(8, Base64::calculateMimeEncodedSize(4));
  EXPECT_EQ(76, Base64::calculateMimeEncodedSize(57));
  EXPECT_EQ(82, Base64::calculateMimeEncodedSize(58));
  EXPECT_EQ(274, Base64::calculateMimeEncodedSize(200));
}

TEST_F(Base64Test, encodeMime) {
  auto encodeMime = [](const std::string& in) {
    size_t len = Base64::calculateMimeEncodedSize(in.size());
    std::string out(len, '\0');
    Base64::encodeMime(in.data(), in.size(), out.data());
    return out;
  };
  EXPECT_EQ("", encodeMime(""));
  EXPECT_EQ("TWFu", encodeMime("Man"));
  EXPECT_EQ("AQ==", encodeMime("\x01"));
  EXPECT_EQ("/+4=", encodeMime("\xff\xee"));
  EXPECT_EQ(
      "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFB",
      encodeMime(std::string(57, 'A')));
  EXPECT_EQ(
      "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFB\r\nQQ==",
      encodeMime(std::string(58, 'A')));
}

} // namespace bytedance::bolt::encoding
