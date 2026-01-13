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

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/flinksql/tests/FlinkFunctionBaseTest.h"
namespace bytedance::bolt::functions::flinksql::test {

namespace {
class StringFunctionsTest : public FlinkFunctionBaseTest {
 public:
  std::optional<bool> isDigit(std::optional<std::string> str) {
    return evaluateOnce<bool>("is_digit(c0)", str);
  }
  std::optional<bool> isAlpha(std::optional<std::string> str) {
    return evaluateOnce<bool>("is_alpha(c0)", str);
  }
  std::optional<bool> isDecimal(std::optional<std::string> str) {
    return evaluateOnce<bool>("is_decimal(c0)", str);
  }
};

TEST_F(StringFunctionsTest, splitIndex) {
  auto splitIndex = [&](std::optional<std::string> input,
                        std::optional<std::string> delim,
                        std::optional<int64_t> index) {
    return evaluateOnce<std::string>(
        "split_index(c0, c1, c2)", input, delim, index);
  };

  auto splitIndexInt = [&](std::optional<std::string> input,
                           std::optional<int32_t> delim,
                           std::optional<int64_t> index) {
    return evaluateOnce<std::string>(
        "split_index(c0, c1, c2)", input, delim, index);
  };

  EXPECT_EQ("he", splitIndex("I,he,she,they", ",", 1));
  EXPECT_EQ(
      "синяя слива",
      splitIndex("синяя сливаలేదా赤いトマトలేదా黃苹果లేదాbrown pear", "లేదా", 0));
  EXPECT_EQ("AQ", splitIndex("AQIDBA==", "I", 0));
  EXPECT_EQ("A", splitIndex("AQIDBA==", "QID", 0));
  EXPECT_EQ("BA==", splitIndex("AQIDBA==", "QID", 1));
  EXPECT_EQ("AQIDBA==", splitIndex("AQIDBA==", "", 0));
  EXPECT_EQ(std::nullopt, splitIndex("AQIDBA==", "I", 2));
  EXPECT_EQ(std::nullopt, splitIndex("AQIDBA==", "I", -1));
  EXPECT_EQ(std::nullopt, splitIndex("AQIDBA==", std::nullopt, 0));
  EXPECT_EQ(std::nullopt, splitIndex("AQIDBA==", std::nullopt, std::nullopt));
  EXPECT_EQ(std::nullopt, splitIndex(std::nullopt, "I", 0));
  EXPECT_EQ("st", splitIndex("Test", "e", 1));
  EXPECT_EQ(std::nullopt, splitIndex(std::nullopt, "e", 1));
  EXPECT_EQ(std::nullopt, splitIndex("test", std::nullopt, 1));
  EXPECT_EQ(std::nullopt, splitIndex("test", "e", -1));
  EXPECT_EQ(std::nullopt, splitIndex("", ",", 0));
  EXPECT_EQ(std::nullopt, splitIndex("", "", 0));
  EXPECT_EQ("AQIDBA==", splitIndex("AQIDBA==", ",", 0));

  EXPECT_EQ("AQ", splitIndexInt("AQIDBA==", 73, 0));
  EXPECT_EQ("DBA==", splitIndexInt("AQIDBA==", 73, 1));
  EXPECT_EQ(std::nullopt, splitIndexInt("AQIDBA==", 73, 2));
  EXPECT_EQ(std::nullopt, splitIndexInt("AQIDBA==", 256, 0));
  EXPECT_EQ(std::nullopt, splitIndexInt("AQIDBA==", 0, 0));
  EXPECT_EQ(std::nullopt, splitIndexInt("AQIDBA==", 'I', std::nullopt));
  EXPECT_EQ(std::nullopt, splitIndexInt("", ',', 0));
}

TEST_F(StringFunctionsTest, isDigit) {
  // basic
  EXPECT_EQ(isDigit("123"), true);
  EXPECT_EQ(isDigit("123a"), false);
  EXPECT_EQ(isDigit("123 "), false);
  EXPECT_EQ(isDigit(" 123"), false);
  EXPECT_EQ(isDigit("123.45"), false);
  EXPECT_EQ(isDigit("123.45e-2"), false);
  EXPECT_EQ(isDigit("a"), false);
  EXPECT_EQ(isDigit("a1"), false);
  EXPECT_EQ(isDigit("1a"), false);
  EXPECT_EQ(isDigit("a1a"), false);
  EXPECT_EQ(isDigit("1a1"), false);
  EXPECT_EQ(isDigit(""), false);
  EXPECT_EQ(isDigit(std::nullopt), false);
  // unicode and emoji
  EXPECT_EQ(isDigit("一"), false);
  EXPECT_EQ(isDigit("😂"), false);
}

TEST_F(StringFunctionsTest, isAlpha) {
  // basic
  EXPECT_EQ(isAlpha("abc"), true);
  EXPECT_EQ(isAlpha("abc123"), false);
  EXPECT_EQ(isAlpha("abc123a"), false);
  EXPECT_EQ(isAlpha("abc123.45"), false);
  EXPECT_EQ(isAlpha("abc123.45e-2"), false);
  EXPECT_EQ(isAlpha("a"), true);
  EXPECT_EQ(isAlpha("a1"), false);
  EXPECT_EQ(isAlpha("1a"), false);
  EXPECT_EQ(isAlpha("a1a"), false);
  EXPECT_EQ(isAlpha("1a1"), false);
  EXPECT_EQ(isAlpha(""), false);
  EXPECT_EQ(isAlpha(std::nullopt), false);
  // unicode and emoji
  EXPECT_EQ(isAlpha("我"), true);
  EXPECT_EQ(isAlpha("你好ABC"), true);
  EXPECT_EQ(isAlpha("你好ABC123"), false);
  EXPECT_EQ(isAlpha("你好ABC123a"), false);
  EXPECT_EQ(isAlpha("你好ABC123.45"), false);
  EXPECT_EQ(isAlpha("😂"), false);
}

TEST_F(StringFunctionsTest, isDecimal) {
  // basic
  EXPECT_EQ(isDecimal("123"), true);
  EXPECT_EQ(isDecimal(" 123"), true);
  EXPECT_EQ(isDecimal("123 "), true);
  EXPECT_EQ(isDecimal("123a"), false);
  EXPECT_EQ(isDecimal("123.45"), true);
  EXPECT_EQ(isDecimal("123.45e-2"), true);
  EXPECT_EQ(isDecimal("-123"), true);
  EXPECT_EQ(isDecimal("+123.45"), true);
  EXPECT_EQ(isDecimal("a"), false);
  EXPECT_EQ(isDecimal("a1"), false);
  EXPECT_EQ(isDecimal("1a"), false);
  EXPECT_EQ(isDecimal("a1a"), false);
  EXPECT_EQ(isDecimal("1a1"), false);
  EXPECT_EQ(isDecimal("inf"), false);
  EXPECT_EQ(isDecimal("nan"), false);
  EXPECT_EQ(isDecimal(""), false);
  EXPECT_EQ(isDecimal(std::nullopt), false);
  // unicode and emoji
  EXPECT_EQ(isDecimal("一"), false);
  EXPECT_EQ(isDecimal("😂"), false);
}
} // namespace
} // namespace bytedance::bolt::functions::flinksql::test
