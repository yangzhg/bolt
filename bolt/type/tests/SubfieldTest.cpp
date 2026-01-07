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

#include "bolt/type/Subfield.h"
#include <gtest/gtest.h>
#include "bolt/type/Tokenizer.h"
using namespace bytedance::bolt::common;

std::vector<std::unique_ptr<Subfield::PathElement>> tokenize(
    const std::string& path) {
  std::vector<std::unique_ptr<Subfield::PathElement>> elements;
  auto tokenizer = Tokenizer::getInstance(path);
  while (tokenizer->hasNext()) {
    elements.push_back(tokenizer->next());
  }
  return elements;
}

void assertInvalidSubfield(
    const std::string& subfield,
    const std::string& message) {
  try {
    tokenize(subfield);
    ASSERT_TRUE(false) << "Expected an exception parsing " << subfield;
  } catch (bytedance::bolt::BoltRuntimeError& e) {
    ASSERT_EQ(e.message(), message);
  }
}

TEST(SubfieldTest, invalidPaths) {
  assertInvalidSubfield("a[b]", "Invalid index b]");
  assertInvalidSubfield("a[2", "Invalid subfield path: a[2^");
  assertInvalidSubfield("a.*", "Invalid subfield path: a.^*");
  assertInvalidSubfield("a[2].[3].", "Invalid subfield path: a[2].^[3].");
}

void testColumnName(const std::string& name) {
  auto elements = tokenize(name);
  EXPECT_EQ(elements.size(), 1);
  EXPECT_EQ(*elements[0].get(), Subfield::NestedField(name));
}

TEST(SubfieldTest, columnNamesWithSpecialCharacters) {
  testColumnName("$bucket");
  testColumnName("apollo-11");
  testColumnName("a/b/c:12");
  testColumnName("@basis");
  testColumnName("@basis|city_id");
}

std::vector<std::unique_ptr<Subfield::PathElement>> createElements() {
  std::vector<std::unique_ptr<Subfield::PathElement>> elements;
  elements.push_back(std::make_unique<Subfield::NestedField>("b"));
  elements.push_back(std::make_unique<Subfield::LongSubscript>(2));
  elements.push_back(std::make_unique<Subfield::LongSubscript>(-1));
  elements.push_back(std::make_unique<Subfield::StringSubscript>("z"));
  elements.push_back(std::make_unique<Subfield::AllSubscripts>());
  elements.push_back(std::make_unique<Subfield::StringSubscript>("34"));
  elements.push_back(std::make_unique<Subfield::StringSubscript>("b \"test\""));
  elements.push_back(std::make_unique<Subfield::StringSubscript>("\"abc"));
  elements.push_back(std::make_unique<Subfield::StringSubscript>("abc\""));
  elements.push_back(std::make_unique<Subfield::StringSubscript>("ab\"cde"));
  return elements;
}

void testRoundTrip(const Subfield& path) {
  auto actual = Subfield(tokenize(path.toString()));
  ASSERT_TRUE(actual.valid());
  EXPECT_EQ(actual, path) << "at " << path.toString() << ", "
                          << actual.toString();
}

TEST(SubfieldTest, basic) {
  auto elements = createElements();
  for (auto& element : elements) {
    std::vector<std::unique_ptr<Subfield::PathElement>> newElements;
    newElements.push_back(std::make_unique<Subfield::NestedField>("a"));
    newElements.push_back(element->clone());
    testRoundTrip(Subfield(std::move(newElements)));
  }

  for (auto& element : elements) {
    for (auto& secondElement : elements) {
      std::vector<std::unique_ptr<Subfield::PathElement>> newElements;
      newElements.push_back(std::make_unique<Subfield::NestedField>("a"));
      newElements.push_back(element->clone());
      newElements.push_back(secondElement->clone());
      testRoundTrip(Subfield(std::move(newElements)));
    }
  }

  for (auto& element : elements) {
    for (auto& secondElement : elements) {
      for (auto& thirdElement : elements) {
        std::vector<std::unique_ptr<Subfield::PathElement>> newElements;
        newElements.push_back(std::make_unique<Subfield::NestedField>("a"));
        newElements.push_back(element->clone());
        newElements.push_back(secondElement->clone());
        newElements.push_back(thirdElement->clone());
        testRoundTrip(Subfield(std::move(newElements)));
      }
    }
  }

  ASSERT_FALSE(Subfield().valid());
  ASSERT_EQ(Subfield().toString(), "");
}

TEST(SubfieldTest, prefix) {
  EXPECT_FALSE(Subfield("a").isPrefix(Subfield("a")));
  EXPECT_TRUE(Subfield("a.b").isPrefix(Subfield("a.b.c")));
  EXPECT_TRUE(Subfield("a.b").isPrefix(Subfield("a.b[1]")));
  EXPECT_TRUE(Subfield("a.b").isPrefix(Subfield("a.b[\"d\"]")));
  EXPECT_FALSE(Subfield("a.c").isPrefix(Subfield("a.b.c")));
  EXPECT_FALSE(Subfield("a.b.c").isPrefix(Subfield("a.b")));
}

TEST(SubfieldTest, hash) {
  std::unordered_set<Subfield> subfields;
  subfields.emplace("a.b");
  subfields.emplace("a[\"b\"]");
  subfields.emplace("a.b.c");
  EXPECT_EQ(subfields.size(), 3);
  EXPECT_TRUE(subfields.find(Subfield("a.b")) != subfields.end());
  subfields.emplace("a.b.c");
  subfields.emplace("a[\"b\"]");
  EXPECT_EQ(subfields.size(), 3);
}

TEST(SubfieldTest, longSubscript) {
  Subfield subfield("a[3309189884973035076]");
  ASSERT_EQ(subfield.path().size(), 2);
  auto* longSubscript =
      dynamic_cast<const Subfield::LongSubscript*>(subfield.path()[1].get());
  ASSERT_TRUE(longSubscript);
  ASSERT_EQ(longSubscript->index(), 3309189884973035076);
}

class FakeTokenizer : public Tokenizer {
 public:
  explicit FakeTokenizer(const std::string& path) : path_(path) {
    state = State::kNotReady;
  }

  bool hasNext() override {
    if (state == State::kDone) {
      return false;
    } else if (state == State::kNotReady) {
      return true;
    }
    BOLT_FAIL("Illegal state");
  }

  std::unique_ptr<Subfield::PathElement> next() override {
    if (!hasNext()) {
      BOLT_USER_FAIL("No more tokens");
    }
    state = State::kDone;
    return std::make_unique<Subfield::NestedField>(path_);
  }

 private:
  const std::string path_;
  State state;
};

TEST(SubfieldTest, CustomTokenizer) {
  Tokenizer::registerInstanceFactory(
      [](const std::string& p) { return std::make_unique<FakeTokenizer>(p); });

  testColumnName("$bucket");
  testColumnName("apollo-11");
  testColumnName("a/b/c:12");
  testColumnName("@basis");
  testColumnName("@basis|city_id");
  testColumnName("city.id@address*:number/date|day$a-b$10_bucket");

  Tokenizer::registerInstanceFactory([](const std::string& p) {
    return std::make_unique<DefaultTokenizer>(p);
  });
}

TEST(SubfieldTest, utf8ColumnNames) {
  // Test simple Chinese column names
  testColumnName("电商赛区");
  testColumnName("用户");
  testColumnName("价格");

  // Test mixed ASCII and Chinese names
  testColumnName("user_标签");
  testColumnName("price_价格");
  testColumnName("标签_tag");

  // Test more complex Chinese names
  testColumnName("中文字段名称");
  testColumnName("用户信息表");

  // Test Chinese names with special characters
  testColumnName("标签-类别");
  testColumnName("用户/组");
  testColumnName("价格:美元");

  // Test nested paths with Chinese characters
  auto elements = tokenize("用户.姓名");
  EXPECT_EQ(elements.size(), 2);
  EXPECT_EQ(*elements[0].get(), Subfield::NestedField("用户"));
  EXPECT_EQ(*elements[1].get(), Subfield::NestedField("姓名"));

  // Test array subscript after Chinese field name
  elements = tokenize("用户[0]");
  EXPECT_EQ(elements.size(), 2);
  EXPECT_EQ(*elements[0].get(), Subfield::NestedField("用户"));
  auto* longSubscript =
      dynamic_cast<const Subfield::LongSubscript*>(elements[1].get());
  ASSERT_TRUE(longSubscript);
  EXPECT_EQ(longSubscript->index(), 0);

  // Test string subscript after Chinese field name
  elements = tokenize("用户[\"姓名\"]");
  EXPECT_EQ(elements.size(), 2);
  EXPECT_EQ(*elements[0].get(), Subfield::NestedField("用户"));
  auto* stringSubscript =
      dynamic_cast<const Subfield::StringSubscript*>(elements[1].get());
  ASSERT_TRUE(stringSubscript);
  EXPECT_EQ(stringSubscript->index(), "姓名");

  // Test complex nested path with Chinese characters
  elements = tokenize("用户.地址[\"城市\"].街道");
  EXPECT_EQ(elements.size(), 4);
  EXPECT_EQ(*elements[0].get(), Subfield::NestedField("用户"));
  EXPECT_EQ(*elements[1].get(), Subfield::NestedField("地址"));
  auto* strSub =
      dynamic_cast<const Subfield::StringSubscript*>(elements[2].get());
  ASSERT_TRUE(strSub);
  EXPECT_EQ(strSub->index(), "城市");
  EXPECT_EQ(*elements[3].get(), Subfield::NestedField("街道"));
}

TEST(SubfieldTest, utf8RoundTrip) {
  // Test round trip for Chinese column names
  testRoundTrip(Subfield("标签"));
  testRoundTrip(Subfield("用户.姓名"));
  testRoundTrip(Subfield("数据[0]"));
  testRoundTrip(Subfield("用户[\"地址\"]"));
  testRoundTrip(Subfield("用户.地址[\"城市\"].街道"));

  // Test complex nested paths with a mix of ASCII and Chinese
  testRoundTrip(Subfield("user.标签"));
  testRoundTrip(Subfield("用户[0].name"));
  testRoundTrip(Subfield("data[\"价格\"].unit"));
  testRoundTrip(Subfield("统计[*].count"));
}

// Test that we can handle potentially problematic Unicode characters
TEST(SubfieldTest, specialUnicodeCharacters) {
  // Test emoji in column names
  testColumnName("😀");
  testColumnName("user_😀");

  // Test supplementary characters
  testColumnName("𠜎𠜱𠝹𠱓");

  // Test combining characters
  testColumnName("é"); // e + acute accent
  testColumnName("ü"); // u + umlaut

  // Test characters from different scripts
  testColumnName("Русский"); // Russian
  testColumnName("हिन्दी"); // Hindi
  testColumnName("日本語"); // Japanese
  testColumnName("한국어"); // Korean
  testColumnName("العربية"); // Arabic (right-to-left)

  // Test mixed scripts
  testColumnName("user_名前_Русский");

  // Test round trip for special characters
  testRoundTrip(Subfield("😀"));
  testRoundTrip(Subfield("😀.count"));
  testRoundTrip(Subfield("user.😀[0]"));
  testRoundTrip(Subfield("𠜎𠜱𠝹𠱓.data"));
}
