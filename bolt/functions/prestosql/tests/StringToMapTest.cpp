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

#include <gtest/gtest.h>
#include <string>
#include <tuple>
#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/lib/string/StringCore.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
using namespace bytedance::bolt::test;
namespace bytedance::bolt::functions::sparksql::test {
namespace {
class StringToMapTest : public SparkFunctionBaseTest {
 protected:
  VectorPtr evaluateStringToMap(const std::vector<StringView>& inputs) {
    const std::string expr =
        fmt::format("str_to_map(c0, '{}', '{}')", inputs[1], inputs[2]);
    return evaluate<MapVector>(
        expr, makeRowVector({makeFlatVector<StringView>({inputs[0]})}));
  }

  void testStringToMap(
      const std::vector<StringView>& inputs,
      const std::vector<std::pair<StringView, std::optional<StringView>>>&
          expect) {
    auto result = evaluateStringToMap(inputs);
    auto expectVector = makeMapVector<StringView, StringView>({expect});
    assertEqualVectors(expectVector, result);
  }

  void setMapKeyDedupPolicy(const std::string& value) {
    queryCtx_->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kSparkMapKeyDedupPolicy, value}});
  }
};

// occurrence >= 20
// '"\\}'
// ',\\s*'
// '\073'
// '"}'
// '\ '
// '\\\.'
// '"}'
// '\_'
// '\&\&'
// '[|]'
// '|'
// '[\.]'
// '['
// ',|，'
// '\\\001'
// '\\\|'
// '\/'
// '\\\$'
// '\\[|,'
// '/\+'
// '],'
TEST_F(StringToMapTest, checkRegexPattern) {
  std::vector<std::tuple<std::string, std::string, bool>> tests{
      {"\\}", "}", false},
      {",\\s*", ",\\s*", true},
      {"\073", "\073", false},
      {"\"}", "\"}", false},
      {"\"}", "\"}", false},
      {"[|]", "[|]", true},
      {"|", "|", true},
      {",|，", ",|，", true},
      {"\\\001", "\\001", true},
      {"\\[|,", "\\[|,", true},
      {"],", "],", true},
      {"\\{", "{", false},
      {"\\}", "}", false},
      {"$_$", "$_$", true},
      {"},", "},", false},
      {"（", "（", false},
      {"）", "）", false},
      {"}\\{", "}{", false},
      {"abc", "abc", false},
      {"a\\*c", "a*c", false},
      {"a\\xc", "a\\xc", true}, // illegal \\x
      {"a*c", "a*c", true},
      {"\\*\\+\\?", "*+?", false},
      {"a\\*b\\xc", "a\\*b\\xc", true}, // illegal \\x
      {"abc字母", "abc字母", false},
      {"\\^\\.\\$\\*\\+\\?\\(\\)\\[\\]\\{\\}\\|", "^.$*+?()[]{}|", false},
      {"\\\\", "\\", false},
      {"\\|\\|", "||", false},
      {"a\\\\*b", "a\\\\*b", true},
      {"\\.", ".", false},
      {"\\*", "*", false},
      {"\\+", "+", false},
      {"\\?", "?", false},
      {"\\(", "(", false},
      {"\\)", ")", false},
      {"\\[", "[", false},
      {"\\]", "]", false},
      {"\\{", "{", false},
      {"\\}", "}", false},
      {"\\|", "|", false},
      {"\\^", "^", false},
      {"\\$", "$", false},
      {"a\\.b", "a.b", false},
      {"a\\*b", "a*b", false},
      {"a\\xb", "a\\xb", true},
      {"", "", false},
      {"\x22", "\x22", false},
      {"a.b", "a.b", true},
      {"a*b", "a*b", true},
      {"\\.\\*", ".*", false},
      {"a\\\\b", "a\\b", false},
      {"a\\b", "a\\b", true},
      {"a.", "a.", true},
      {"'a'", "'a'", false},
      {"\"a\"", "\"a\"", false},
      {"a b", "a b", false},
      {"a \\ b", "a \\ b", true},
      {"a\nb", "a\nb", false},
      {"a\\\nb", "a\\\nb", true},
      {"a\tb", "a\tb", false},
      {"a\\\tb", "a\\\tb", true},
      {"a\rb", "a\rb", false},
      {"a\\\rb", "a\\\rb", true},
      {"a\fb", "a\fb", false},
      {"a\\\fb", "a\\\fb", true},
      {"a\vb", "a\vb", false},
      {"a\\\vb", "a\\\vb", true},
      {"[a]", "[a]", true},
      {"[a-b]", "[a-b]", true},
      {"[a-z]", "[a-z]", true},
      {"[a-zA-Z]", "[a-zA-Z]", true},
      {"[a-zA-Z0-9]", "[a-zA-Z0-9]", true},
      {"[a-zA-Z0-9_]", "[a-zA-Z0-9_]", true},
      {"[a-zA-Z0-9_\\-]", "[a-zA-Z0-9_\\-]", true},
      {"[a-zA-Z0-9_\\-\\.]", "[a-zA-Z0-9_\\-\\.]", true},
      {"()", "()", true},
      {"()()", "()()", true},
      {"()()()", "()()()", true},
      {"(())", "(())", true},
      {"(())()", "(())()", true},
      {"abc\\", "abc\\", true},
      {"\\x", "\\x", true},
      {"a\\1", "a\\1", true},
      {"{}", "{}", true},
      {"{a}", "{a}", true},
      {"{a,b}", "{a,b}", true},
      {"][]", "][]", true},
      {"[]", "[]", true},
      {"][", "][", true},
      {")(", ")(", true},
  };
  for (size_t i = 0; i < tests.size(); ++i) {
    auto& [input, expected, flag] = tests[i];
    auto isRegex = stringCore::checkRegexPattern(input);
    EXPECT_EQ(isRegex, flag)
        << i << " -th input: " << input << ", expected: " << expected;
    if (!isRegex) {
      EXPECT_EQ(input, expected)
          << i << " -th input: " << input << ", expected: " << expected;
    }
  }
}

TEST_F(StringToMapTest, basic) {
  testStringToMap(
      {"a:1,b:2,c:3", ",", ":"}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});
  testStringToMap({"a: ,b:2", ",", ":"}, {{"a", " "}, {"b", "2"}});
  testStringToMap({"a:,b:2", ",", ":"}, {{"a", ""}, {"b", "2"}});
  testStringToMap({"", ",", ":"}, {{"", std::nullopt}});
  testStringToMap({"a", ",", ":"}, {{"a", std::nullopt}});
  testStringToMap(
      {"a=1,b=2,c=3", ",", "="}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});
  testStringToMap({"", ",", "="}, {{"", std::nullopt}});
  testStringToMap(
      {"a::1,b::2,c::3", ",", "c"},
      {{"a::1", std::nullopt}, {"b::2", std::nullopt}, {"", "::3"}});
  testStringToMap(
      {"a:1_b:2_c:3", "_", ":"}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});

  // Same delimiters.
  testStringToMap(
      {"a:1,b:2,c:3", ",", ","},
      {{"a:1", std::nullopt}, {"b:2", std::nullopt}, {"c:3", std::nullopt}});
  testStringToMap(
      {"a:1_b:2_c:3", "_", "_"},
      {{"a:1", std::nullopt}, {"b:2", std::nullopt}, {"c:3", std::nullopt}});

  // Exception for illegal delimiters.
  // Empty string is used.
  BOLT_ASSERT_THROW(
      evaluateStringToMap({"a:1,b:2", "", ":"}),
      "pairDelimiter's size should be greater than or equal to 1, actual is 0.");
  BOLT_ASSERT_THROW(
      evaluateStringToMap({"a:1,b:2", ",", ""}),
      "keyValueDelimiter's size should be greater than or equal to 1, actual is 0.");
  // Delimiter's length > 1.
  testStringToMap({"a:1,b:2", ",,", ":"}, {{"a", "1,b:2"}});
  testStringToMap(
      {"a:1,b:2", ",", "::"}, {{"a:1", std::nullopt}, {"b:2", std::nullopt}});
  // Unicode character is used.
  testStringToMap({"a:1,b:2", "å", ":"}, {{"a", "1,b:2"}});
  testStringToMap(
      {"a:1,b:2", ",", "æ"}, {{"a:1", std::nullopt}, {"b:2", std::nullopt}});

  {
    auto result = evaluate<MapVector>(
        "str_to_map(c0, ',', ':')",
        makeRowVector(
            {makeNullableFlatVector<StringView>({std::nullopt, "a:1,b:2"})}));
    auto expectVector = makeNullableMapVector<StringView, StringView>(
        {std::nullopt, {{{"a", "1"}, {"b", "2"}}}});
    assertEqualVectors(expectVector, result);
  }

  // Exception for duplicated keys.
  BOLT_ASSERT_THROW(
      evaluateStringToMap({"a:1,b:2,a:3", ",", ":"}),
      "Duplicate map key a was found, please check the input data. If you want "
      "to remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to "
      "LAST_WIN so that the key inserted at last takes precedence.");
  BOLT_ASSERT_THROW(
      evaluateStringToMap({":1,:2", ",", ":"}),
      "Duplicate map key  was found, please check the input data. If you want "
      "to remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to "
      "LAST_WIN so that the key inserted at last takes precedence.");
}

TEST_F(StringToMapTest, mapKeyDedupPolicy) {
  setMapKeyDedupPolicy("EXCEPTION");
  BOLT_ASSERT_THROW(
      evaluateStringToMap({"a:1,b:2,a:3", ",", ":"}),
      "Duplicate map key a was found, please check the input data. If you want "
      "to remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to "
      "LAST_WIN so that the key inserted at last takes precedence.");
  setMapKeyDedupPolicy("FIRST_WIN");
  testStringToMap({"a:1,b:2,a:3", ",", ":"}, {{"a", "1"}, {"b", "2"}});
  setMapKeyDedupPolicy("LAST_WIN");
  testStringToMap({"a:1,b:2,a:3", ",", ":"}, {{"a", "3"}, {"b", "2"}});
}
TEST_F(StringToMapTest, basicRegexDelimiters) {
  // Basic cases with regex delimiters
  testStringToMap(
      {"a:1,b:2,c:3", "[,]", "[:;]"}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});
  testStringToMap(
      {"a:1,b:2,c;3", "[,]", "[:;]"}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});
  testStringToMap(
      {"a:1;b:2;c:3", "[,;]", "[:;]"}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});

  // Edge cases with regex delimiters
  BOLT_ASSERT_THROW(
      evaluateStringToMap({"a1b2c3", "1|2", "a|b|c"}),
      "Duplicate map key  was found, please check the input data. If you want "
      "to remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to "
      "LAST_WIN so that the key inserted at last takes precedence.");

  testStringToMap(
      {"a::1;b::2;c::3", "[,;]", "::"}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});
  testStringToMap({"a:1b:2c:3", "b", "[:;]"}, {{"", "2c:3"}, {"a", "1"}});

  // Same delimiters
  testStringToMap(
      {"a:1,b:2,c:3", "[,]", "[,]"},
      {{"a:1", std::nullopt}, {"b:2", std::nullopt}, {"c:3", std::nullopt}});
  testStringToMap(
      {"a:1_b:2_c:3", "[_]", "[_]"},
      {{"a:1", std::nullopt}, {"b:2", std::nullopt}, {"c:3", std::nullopt}});

  // Invalid delimiters
  BOLT_ASSERT_THROW(
      evaluateStringToMap({"a:1,b:2", "", "[:;]"}),
      "pairDelimiter's size should be greater than or equal to 1, actual is 0.");
  BOLT_ASSERT_THROW(
      evaluateStringToMap({"a:1,b:2", "[,]", ""}),
      "keyValueDelimiter's size should be greater than or equal to 1, actual is 0.");

  // Unicode character delimiters
  testStringToMap({"a:1,b:2", "[å]", "[:;]"}, {{"a", "1,b:2"}});
  testStringToMap(
      {"a:1,b:2", "[,]", "[æ]"},
      {{"a:1", std::nullopt}, {"b:2", std::nullopt}});

  // Nullable input handling
  {
    auto result = evaluate<MapVector>(
        "str_to_map(c0, '[,]', '[:;]')",
        makeRowVector(
            {makeNullableFlatVector<StringView>({std::nullopt, "a:1,b:2"})}));
    auto expectVector = makeNullableMapVector<StringView, StringView>(
        {std::nullopt, {{{"a", "1"}, {"b", "2"}}}});
    assertEqualVectors(expectVector, result);
  }

  // Exception for duplicated keys
  BOLT_ASSERT_THROW(
      evaluateStringToMap({"a:1,b:2,a:3", "[,]", "[:;]"}),
      "Duplicate map key a was found, please check the input data. If you want "
      "to remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to "
      "LAST_WIN so that the key inserted at last takes precedence.");
  BOLT_ASSERT_THROW(
      evaluateStringToMap({":1,:2", "[,]", "[:;]"}),
      "Duplicate map key  was found, please check the input data. If you want "
      "to remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to "
      "LAST_WIN so that the key inserted at last takes precedence.");
}

TEST_F(StringToMapTest, mapKeyDedupPolicyWithRegex) {
  setMapKeyDedupPolicy("EXCEPTION");
  BOLT_ASSERT_THROW(
      evaluateStringToMap({"a:1,b:2,a:3", "[,]", "[:;]"}),
      "Duplicate map key a was found, please check the input data. If you want "
      "to remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to "
      "LAST_WIN so that the key inserted at last takes precedence.");
  setMapKeyDedupPolicy("FIRST_WIN");
  testStringToMap({"a:1,b:2,a:3", "[,]", "[:;]"}, {{"a", "1"}, {"b", "2"}});
  setMapKeyDedupPolicy("LAST_WIN");
  testStringToMap({"a:1,b:2,a:3", "[,]", "[:;]"}, {{"a", "3"}, {"b", "2"}});
}
TEST_F(StringToMapTest, regexDelimitersWithEscape) {
  // Cases with regex special characters needing escape
  testStringToMap(
      {"a:1|b:2|c:3", "\\|", ":"}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});
  testStringToMap(
      {"a:1?b:2?c:3", "\\?", ":"}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});
  testStringToMap(
      {"a:1*b:2*c:3", "\\*", ":"}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});

  // Cases with multiple special characters needing escape
  testStringToMap(
      {"a:1|b:2|c:3", "\\|", "\\:"}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});
  testStringToMap(
      {"a=1?b=2?c=3", "\\?", "\\="}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});

  // Edge cases with special characters
  testStringToMap(
      {"a|1,b|2,c|3", ",", "\\|"}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});
  testStringToMap(
      {"a\\1;b\\2;c\\3", ";", "\\\\"}, {{"a", "1"}, {"b", "2"}, {"c", "3"}});

  // Nullable input handling with special characters
  {
    auto result = evaluate<MapVector>(
        "str_to_map(c0, '\\|', ':')",
        makeRowVector(
            {makeNullableFlatVector<StringView>({std::nullopt, "a:1|b:2"})}));
    auto expectVector = makeNullableMapVector<StringView, StringView>(
        {std::nullopt, {{{"a", "1"}, {"b", "2"}}}});
    assertEqualVectors(expectVector, result);
  }

  // Exception for duplicated keys with special characters
  BOLT_ASSERT_THROW(
      evaluateStringToMap({"a:1|b:2|a:3", "\\|", ":"}),
      "Duplicate map key a was found, please check the input data. If you want "
      "to remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to "
      "LAST_WIN so that the key inserted at last takes precedence.");
  BOLT_ASSERT_THROW(
      evaluateStringToMap({":1|:2", "\\|", ":"}),
      "Duplicate map key  was found, please check the input data. If you want "
      "to remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to "
      "LAST_WIN so that the key inserted at last takes precedence.");
}

TEST_F(StringToMapTest, mapKeyDedupPolicyWithRegexEscape) {
  setMapKeyDedupPolicy("EXCEPTION");
  BOLT_ASSERT_THROW(
      evaluateStringToMap({"a:1|b:2|a:3", "\\|", ":"}),
      "Duplicate map key a was found, please check the input data. If you want "
      "to remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to "
      "LAST_WIN so that the key inserted at last takes precedence.");
  setMapKeyDedupPolicy("FIRST_WIN");
  testStringToMap({"a:1|b:2|a:3", "\\|", ":"}, {{"a", "1"}, {"b", "2"}});
  setMapKeyDedupPolicy("LAST_WIN");
  testStringToMap({"a:1|b:2|a:3", "\\|", ":"}, {{"a", "3"}, {"b", "2"}});
}

TEST_F(StringToMapTest, flinkCompatible) {
  queryCtx_->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kEnableFlinkCompatible, "true"}});
  testStringToMap({"", ",", ":"}, {});
  testStringToMap({"a", ",", ":"}, {{"a", std::nullopt}});
  testStringToMap({"k1=v1,k2=v2", ",", "="}, {{"k1", "v1"}, {"k2", "v2"}});
}
} // namespace
} // namespace bytedance::bolt::functions::sparksql::test