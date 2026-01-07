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
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class Base64Test : public SparkFunctionBaseTest {
 protected:
  std::optional<std::string> base64(const std::optional<std::string>& a) {
    return evaluateOnce<std::string>("base64(cast (c0 as varbinary))", a);
  }
};

TEST_F(Base64Test, basic) {
  EXPECT_EQ(base64(std::nullopt), std::nullopt);
  EXPECT_EQ(base64("Man"), "TWFu");
  EXPECT_EQ(base64("\x01"), "AQ==");
  EXPECT_EQ(base64("\xff\xee"), "/+4=");
  EXPECT_EQ(base64("hello world"), "aGVsbG8gd29ybGQ=");
  EXPECT_EQ(base64("Spark SQL"), "U3BhcmsgU1FM");
  EXPECT_EQ(
      base64(std::string(57, 'A')),
      "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFB");
  EXPECT_EQ(
      base64(std::string(58, 'A')),
      "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFB\r\nQQ==");
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
