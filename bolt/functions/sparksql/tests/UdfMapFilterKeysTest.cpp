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

#include <common/base/tests/GTestUtils.h>
#include <vector/BaseVector.h>
#include <vector/ComplexVector.h>
#include <vector/tests/utils/VectorTestBase.h>
#include <cstdint>
#include <optional>
#include "bolt/dwio/common/tests/utils/BatchMaker.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
namespace bytedance::bolt::functions::sparksql::test {
using namespace bytedance::bolt::test;
namespace {

class UdfMapFilterKeysTest : public SparkFunctionBaseTest {
 protected:
  template <typename K, typename V>
  void checkMapFilterKeys(BaseVector* expected, const BaseVector& result) {}

  template <typename TKey, typename TValue>
  static std::vector<TKey> mapKeys(const std::map<TKey, TValue>& m) {
    std::vector<TKey> keys;
    keys.reserve(m.size());
    for (const auto& [key, value] : m) {
      keys.push_back(key);
    }
    return keys;
  }

  template <typename TKey, typename TValue>
  static std::vector<TValue> mapValues(const std::map<TKey, TValue>& m) {
    std::vector<TValue> values;
    values.reserve(m.size());
    for (const auto& [key, value] : m) {
      values.push_back(value);
    }
    return values;
  }

  MapVectorPtr makeMapVector(
      vector_size_t size,
      const std::map<std::string, int32_t>& m,
      std::function<bool(vector_size_t /*row*/)> isNullAt = nullptr) {
    std::vector<std::string> keys = mapKeys(m);
    std::vector<int32_t> values = mapValues(m);
    return vectorMaker_.mapVector<StringView, int32_t>(
        size,
        [&](vector_size_t /*row*/) { return keys.size(); },
        [&](vector_size_t /*mapRow*/, vector_size_t row) {
          return StringView(keys[row]);
        },
        [&](vector_size_t mapRow, vector_size_t row) { return values[row]; },
        isNullAt);
  }

  ArrayVectorPtr makeArrayVector(
      vector_size_t size,
      const std::vector<std::string>& a,
      std::function<bool(vector_size_t /*row*/)> isNullAt = nullptr) {
    return vectorMaker_.arrayVector<StringView>(
        size,
        [&](vector_size_t /*row*/) { return a.size(); },
        [&](vector_size_t /*idx*/, vector_size_t row) {
          return StringView(a[row]);
        },
        isNullAt);
  }

  void assertMapVectorEqulas(
      MapVectorPtr expectedMap,
      MapVectorPtr result,
      vector_size_t size) {
    ASSERT_EQ(result->size(), size);
    ASSERT_EQ(expectedMap->size(), size);
    for (vector_size_t i = 0; i < size; i++) {
      ASSERT_TRUE(expectedMap->equalValueAt(result.get(), i, i))
          << "at " << i << ": expected " << expectedMap->toString(i) << ", got "
          << result->toString(i);
    }
  }
};

TEST_F(UdfMapFilterKeysTest, basic) {
  vector_size_t size = 10;

  std::map<std::string, int32_t> a = {{"a1", 1}, {"a2", 2}, {"a3", 3}};
  std::vector<std::string> b = {"a1", "a2"};
  // auto aMap = makeMapVector(size, a, nullEvery(5));
  auto aMap = makeMapVector(size, a);
  auto bArray = makeArrayVector(size, b);
  auto result = evaluate<MapVector>(
      "map_filter_keys(c0, c1)", makeRowVector({aMap, bArray}));
  std::map<std::string, int32_t> expected = {{"a1", 1}, {"a2", 2}};
  // auto expectedMap = makeMapVector(size, expected, nullEvery(5));
  auto expectedMap = makeMapVector(size, expected);
  assertMapVectorEqulas(expectedMap, result, size);
  assertEqualVectors(expectedMap, result);
}

TEST_F(UdfMapFilterKeysTest, basic_constant_array) {
  vector_size_t size = 10;

  std::map<std::string, int32_t> a = {{"a1", 1}, {"a2", 2}, {"a3", 3}};
  std::vector<std::string> b = {"a1", "a2"};
  auto aMap = makeMapVector(size, a, nullEvery(5));
  auto bArray = makeConstantArray(size, b);
  auto result = evaluate<MapVector>(
      "map_filter_keys(c0, c1)", makeRowVector({aMap, bArray}));
  std::map<std::string, int32_t> expected = {{"a1", 1}, {"a2", 2}};
  auto expectedMap = makeMapVector(size, expected, nullEvery(5));
  assertMapVectorEqulas(expectedMap, result, size);
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
