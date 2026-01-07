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
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
namespace bytedance::bolt::functions::sparksql::test {
using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
namespace {

class MapUpdateTest : public FunctionBaseTest {
 protected:
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
        [&](vector_size_t mapRow, vector_size_t row) {
          return mapRow % 11 + values[row];
        },
        isNullAt);
  }
  FlatVectorPtr<EvalType<bool>> makeFlatVector(
      vector_size_t size,
      bool value,
      std::function<bool(vector_size_t /*row*/)> isNullAt = nullptr) {
    return vectorMaker_.flatVector<bool>(
        size, [&](vector_size_t /*row*/) { return value; }, isNullAt);
  }

  template <typename TKey, typename TValue>
  std::map<TKey, TValue> update(
      const std::map<TKey, TValue>& a,
      const std::map<TKey, TValue>& b) {
    std::map<TKey, TValue> result;
    result.insert(b.begin(), b.end());
    result.insert(a.begin(), a.end());
    return result;
  }
};
TEST_F(MapUpdateTest, basic_with_insert) {
  vector_size_t size = 1'000;
  std::map<std::string, int32_t> a = {{"a1", 1}, {"a2", 2}, {"a3", 3}};
  std::map<std::string, int32_t> b = {
      {"b1", 1}, {"b2", 2}, {"b3", 3}, {"b4", 4}};
  auto aMap = makeMapVector(size, a, nullEvery(5));
  auto bMap = makeMapVector(size, b);
  std::map<std::string, int32_t> ab = update(a, b);
  auto expectedMap =
      makeMapVector(size, ab, [](vector_size_t row) { return row % 5 == 0; });

  auto result = evaluate<MapVector>(
      "map_update(c0, c1, true)", makeRowVector({aMap, bMap}));
  for (vector_size_t i = 0; i < size; i++) {
    ASSERT_TRUE(expectedMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedMap->toString(i) << ", got "
        << result->toString(i);
  }
}

TEST_F(MapUpdateTest, basic_without_insert_one) {
  vector_size_t size = 1'000;
  std::map<std::string, int32_t> a = {{"a1", 1}, {"a2", 2}, {"a3", 3}};
  std::map<std::string, int32_t> b = {
      {"b1", 1}, {"b2", 2}, {"b3", 3}, {"b4", 4}};
  auto aMap = makeMapVector(size, a, nullEvery(5));
  auto bMap = makeMapVector(size, b);
  auto expectedMap =
      makeMapVector(size, a, [](vector_size_t row) { return row % 5 == 0; });

  auto result = evaluate<MapVector>(
      "map_update(c0, c1, false)", makeRowVector({aMap, bMap}));
  for (vector_size_t i = 0; i < size; i++) {
    ASSERT_TRUE(expectedMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedMap->toString(i) << ", got "
        << result->toString(i);
  }
  result =
      evaluate<MapVector>("map_update(c0, c1)", makeRowVector({aMap, bMap}));
  for (vector_size_t i = 0; i < size; i++) {
    ASSERT_TRUE(expectedMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedMap->toString(i) << ", got "
        << result->toString(i);
  }
}

TEST_F(MapUpdateTest, basic_without_insert_two) {
  vector_size_t size = 1'000;
  std::map<std::string, int32_t> a = {{"a1", 1}, {"a2", 2}, {"a3", 3}};
  std::map<std::string, int32_t> b = {
      {"a1", 10}, {"a3", 30}, {"a4", 40}, {"a5", 50}};
  std::map<std::string, int32_t> r1 = {{"a1", 10}, {"a2", 2}, {"a3", 30}};
  std::map<std::string, int32_t> r2 = {
      {"a1", 10}, {"a2", 2}, {"a3", 30}, {"a4", 40}, {"a5", 50}};

  auto aMap = makeMapVector(size, a, nullEvery(5));
  auto bMap = makeMapVector(size, b);
  auto expectedMap =
      makeMapVector(size, r1, [](vector_size_t row) { return row % 5 == 0; });

  auto result =
      evaluate<MapVector>("map_update(c0, c1)", makeRowVector({aMap, bMap}));
  for (vector_size_t i = 0; i < size; i++) {
    ASSERT_TRUE(expectedMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedMap->toString(i) << ", got "
        << result->toString(i);
  }

  expectedMap =
      makeMapVector(size, r2, [](vector_size_t row) { return row % 5 == 0; });
  result = evaluate<MapVector>(
      "map_update(c0, c1, true)", makeRowVector({aMap, bMap}));
  for (vector_size_t i = 0; i < size; i++) {
    ASSERT_TRUE(expectedMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedMap->toString(i) << ", got "
        << result->toString(i);
  }
}

TEST_F(MapUpdateTest, basic_null_first) {
  vector_size_t size = 1'000;
  std::map<std::string, int32_t> a = {{"a1", 1}, {"a2", 2}, {"a3", 3}};
  std::map<std::string, int32_t> b = {};
  std::map<std::string, int32_t> r = {{"a1", 1}, {"a2", 2}, {"a3", 3}};

  auto aMap = makeMapVector(size, a, nullEvery(5));
  auto bMap = makeMapVector(size, b);
  auto expectedMap =
      makeMapVector(size, r, [](vector_size_t row) { return row % 5 == 0; });

  auto result =
      evaluate<MapVector>("map_update(c0, c1)", makeRowVector({aMap, bMap}));
  for (vector_size_t i = 0; i < size; i++) {
    ASSERT_TRUE(expectedMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedMap->toString(i) << ", got "
        << result->toString(i);
  }
}

TEST_F(MapUpdateTest, basic_null_second) {
  vector_size_t size = 1'000;
  std::map<std::string, int32_t> a = {};
  std::map<std::string, int32_t> b = {{"a1", 1}, {"a2", 2}, {"a3", 3}};
  std::map<std::string, int32_t> r = {};

  auto aMap = makeMapVector(size, a, nullEvery(5));
  auto bMap = makeMapVector(size, b);
  auto expectedMap =
      makeMapVector(size, r, [](vector_size_t row) { return row % 5 == 0; });

  auto result =
      evaluate<MapVector>("map_update(c0, c1)", makeRowVector({aMap, bMap}));
  for (vector_size_t i = 0; i < size; i++) {
    ASSERT_TRUE(expectedMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedMap->toString(i) << ", got "
        << result->toString(i);
  }
}

TEST_F(MapUpdateTest, basic_null_third) {
  vector_size_t size = 1'000;
  std::map<std::string, int32_t> a = {{"a1", 1}, {"a2", 2}, {"a3", 3}};
  std::map<std::string, int32_t> b = {
      {"b1", 1}, {"b2", 2}, {"b3", 3}, {"b4", 4}};
  auto aMap = makeMapVector(size, a, nullEvery(5));
  auto bMap = makeMapVector(size, b);
  auto cVec = makeAllNullFlatVector<bool>(size);
  auto expectedMap =
      makeMapVector(size, a, [](vector_size_t row) { return row % 5 == 0; });

  ASSERT_ANY_THROW(evaluate<MapVector>(
      "map_update(c0, c1, c2)", makeRowVector({aMap, bMap, cVec})));
}

TEST_F(MapUpdateTest, basic_flat_third) {
  vector_size_t size = 1'000;
  std::map<std::string, int32_t> a = {{"a1", 1}, {"a2", 2}, {"a3", 3}};
  std::map<std::string, int32_t> b = {
      {"b1", 1}, {"b2", 2}, {"b3", 3}, {"b4", 4}};
  auto aMap = makeMapVector(size, a, nullEvery(5));
  auto bMap = makeMapVector(size, b);
  auto cVec = makeFlatVector(size, true);
  std::map<std::string, int32_t> ab = update(a, b);
  auto expectedMap =
      makeMapVector(size, ab, [](vector_size_t row) { return row % 5 == 0; });

  auto result = evaluate<MapVector>(
      "map_update(c0, c1, c2)", makeRowVector({aMap, bMap, cVec}));
  for (vector_size_t i = 0; i < size; i++) {
    ASSERT_TRUE(expectedMap->equalValueAt(result.get(), i, i))
        << "at " << i << ": expected " << expectedMap->toString(i) << ", got "
        << result->toString(i);
  }
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
