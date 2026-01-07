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

#include <optional>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/expression/Expr.h"
#include "bolt/functions/lib/SubscriptUtil.h"
#include "bolt/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "bolt/vector/BaseVector.h"
#include "vector/VectorPrinter.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::functions::test;
using namespace bytedance::bolt::test;
using bytedance::bolt::functions::SubscriptImpl;

namespace {
class ElementTest : public FunctionBaseTest {
 protected:
  template <typename T>
  void testElement(const VectorPtr& input, const VectorPtr& expected) {
    auto result =
        evaluate<SimpleVector<T>>("element(c0)", makeRowVector({input}));
    assertEqualVectors(expected, result);
  }
  template <typename T>
  void testError(const VectorPtr& input, const std::string& message) {
    try {
      evaluate<SimpleVector<T>>("element(c0)", makeRowVector({input}));
      EXPECT_TRUE(false) << "Expected exception";
    } catch (const BoltUserError& e) {
      ASSERT_TRUE(e.message().find(message) != std::string::npos)
          << e.message();
    }
  }
};
TEST_F(ElementTest, basic) {
  // empty array
  auto emptyArray = makeArrayVector<int32_t>({{}});
  auto expectedNull = makeNullableFlatVector<int32_t>({std::nullopt});
  testElement<int32_t>(emptyArray, expectedNull);

  // test single element
  auto singleElementArray = makeArrayVector<int32_t>({{42}});
  auto expectedValue = makeNullableFlatVector<int32_t>({42});
  testElement<int32_t>(singleElementArray, expectedValue);

  // test multiple elements
  auto multiElementArray = makeArrayVector<int32_t>({{1, 2}});
  testError<int32_t>(
      multiElementArray,
      "Array element function only supports arrays of size 1, but got 2");
}

TEST_F(ElementTest, nullHandling) {
  // test null array
  auto arrayWithNull = makeNullableArrayVector<int32_t>(
      {std::vector<std::optional<int32_t>>{1},
       {std::nullopt},
       {},
       std::nullopt});
  auto expectedNull = makeNullableFlatVector<int32_t>(
      {1, std::nullopt, std::nullopt, std::nullopt});
  testElement<int32_t>(arrayWithNull, expectedNull);
}

TEST_F(ElementTest, differentTypes) {
  // test String type
  auto stringArray = makeArrayVector<StringView>({{"bolt"}});
  auto expectedString = makeNullableFlatVector<StringView>({"bolt"});
  testElement<StringView>(stringArray, expectedString);
}

TEST_F(ElementTest, arrayWithDictionaryElements) {
  {
    auto elementsIndices = makeIndices({6, 5, 4, 3, 2, 1, 0});
    auto elements = wrapInDictionary(
        elementsIndices, makeFlatVector<int64_t>({0, 1, 2, 3, 4, 5, 6}));

    // array vector is [[6], [5], [4], [3], [2], [1], [0]].
    auto inputArray = makeArrayVector({0, 1, 2, 3, 4, 5, 6}, elements);
    auto expected = makeFlatVector<int64_t>({6, 5, 4, 3, 2, 1, 0});

    auto result = evaluate("element(c0)", makeRowVector({inputArray}));
    test::assertEqualVectors(expected, result);
  }
}
TEST_F(ElementTest, arrayWithNestedArray) {
  auto nestedArray = makeNestedArrayVectorFromJson<int32_t>({
      "[[1, 2, 3]]",
      "[[4, 4]]",
      "null",
  });
  auto expectedNull = makeNullableArrayVector<int32_t>(
      {std::vector<std::optional<int32_t>>{1, 2, 3},
       std::vector<std::optional<int32_t>>{4, 4},
       std::nullopt});
  auto result = evaluate("element(c0)", makeRowVector({nestedArray}));
  assertEqualVectors(expectedNull, result);
}
} // namespace