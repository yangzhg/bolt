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

#include <DataSketches/frequent_items_sketch.hpp>

#include "SketchTestBase.hpp"
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
namespace bytedance::bolt::aggregate::sketches::test {

class SketchFITests : public SketchTestBase {};

TEST_F(SketchFITests, example) {
  // Publicly documented fi data sketch example, guaranteeing correct linking
  // https://datasketches.apache.org/docs/Frequency/FrequentItemsCppExample.html

  // this section generates two sketches and serializes them into files
  {
    datasketches::frequent_items_sketch<std::string> sketch1(64);
    sketch1.update("a");
    sketch1.update("a");
    sketch1.update("b");
    sketch1.update("c");
    sketch1.update("a");
    sketch1.update("d");
    sketch1.update("a");
    std::ofstream os1("freq_str_sketch1.bin");
    sketch1.serialize(os1);

    datasketches::frequent_items_sketch<std::string> sketch2(64);
    sketch2.update("e");
    sketch2.update("a");
    sketch2.update("f");
    sketch2.update("f");
    sketch2.update("f");
    sketch2.update("g");
    sketch2.update("a");
    sketch2.update("f");
    std::ofstream os2("freq_str_sketch2.bin");
    sketch2.serialize(os2);
  }

  // this section deserializes the sketches, produces a union and prints the
  // result
  {
    std::ifstream is1("freq_str_sketch1.bin");
    auto sketch1 =
        datasketches::frequent_items_sketch<std::string>::deserialize(is1);

    std::ifstream is2("freq_str_sketch2.bin");
    auto sketch2 =
        datasketches::frequent_items_sketch<std::string>::deserialize(is2);

    // we could merge sketch2 into sketch1 or the other way around
    // this is an example of using a new sketch as a union and keeping the
    // original sketches intact
    datasketches::frequent_items_sketch<std::string> u(64);
    u.merge(sketch1);
    u.merge(sketch2);

    auto items = u.get_frequent_items(datasketches::NO_FALSE_POSITIVES);
    EXPECT_EQ(items.size(), 7);
    EXPECT_EQ(items[0].get_item(), "a");
    EXPECT_EQ(items[0].get_estimate(), 6);
    EXPECT_EQ(items[0].get_lower_bound(), 6);
    EXPECT_EQ(items[0].get_upper_bound(), 6);
  }
}

TEST_F(SketchFITests, singleAggregation) {
  vector_size_t size = 10'000;
  auto values = makeFlatVector<double>(
      size, [](auto row) { return (row % 4) < 3 ? 1.0 : 2.0; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .singleAggregation({}, {"fi(c0, 64)"})
                .planNode();

  double result = readSingleValue(op).value<double>();
  EXPECT_EQ(result, 1.0);
}

TEST_F(SketchFITests, basic) {
  vector_size_t size = 10'000;
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return (row % 4) < 3 ? 1 : 2; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .partialAggregation({}, {"fi(c0, 64)"})
                .finalAggregation()
                .planNode();
  int result = readSingleValue(op).value<int>();
  EXPECT_EQ(result, 1);
}

TEST_F(SketchFITests, globalAggIntegers) {
  vector_size_t size = 1'000;
  auto values =
      makeFlatVector<int>(size, [](auto row) { return (row % 4) < 3 ? 5 : 6; });

  testGlobalAgg<int>(values, "fi(c0, 32)", 5, 5);
}

TEST_F(SketchFITests, globalAggStrings) {
  vector_size_t size = 1'000;

  auto values = makeFlatVector<StringView>(size, [&](auto row) {
    return StringView(kFruits[row % kFruits.size()]);
  });

  testGlobalAgg(
      values,
      "fi(c0, 32)",
      std::string("grapefruit"),
      std::string("grapefruit"));
}

TEST_F(SketchFITests, globalAggHighCardinalityIntegers) {
  vector_size_t size = 100'000;
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return (row % 4) < 3 ? INT32_MAX : INT32_MIN; });

  testGlobalAgg(values, "fi(c0, 32)", INT32_MAX, INT32_MAX);
}

TEST_F(SketchFITests, globalAggVeryLowCardinalityIntegers) {
  vector_size_t size = 20;
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return (row % 4) < 3 ? 1 : 2; });

  testGlobalAgg(values, "fi(c0, 32)", 1, 1);
}

TEST_F(SketchFITests, globalAggIntegersWithDifferentLogMaxMapSize) {
  vector_size_t size = 10'000;
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return (row % 4) < 3 ? 1 : 2; });

  testGlobalAgg(values, "fi(c0, 64)", 1, 1);
  testGlobalAgg(values, "fi(c0, 32)", 1, 1);
  testGlobalAgg(values, "fi(c0, 16)", 1, 1);
}

TEST_F(SketchFITests, groupByIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : 30; });

  testGroupByAgg<int32_t, int32_t>(
      keys, values, "fi(c1, 32)", {{0, 27}, {1, 30}}, {{0, 27}, {1, 30}});
}

TEST_F(SketchFITests, groupByStrings) {
  vector_size_t size = 1'000;

  auto keys = makeFlatVector<StringView>(size, [&](auto row) {
    return StringView(row % 2 == 0 ? kFruits[0] : kVegetables[0]);
  });
  auto values = makeFlatVector<StringView>(size, [&](auto row) {
    return StringView(row % 2 == 0 ? kFruits[1] : kVegetables[1]);
  });

  testGroupByAgg<std::string, std::string>(
      keys,
      values,
      "fi(c1, 32)",
      {{kFruits[0], kFruits[1]}, {kVegetables[0], kVegetables[1]}},
      {{kFruits[0], kFruits[1]}, {kVegetables[0], kVegetables[1]}});
}

TEST_F(SketchFITests, groupByHighCardinalityIntegers) {
  vector_size_t size = 1'0;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? INT32_MAX : INT32_MIN; });

  testGroupByAgg<int, int>(
      keys,
      values,
      "fi(c1, 32)",
      {{0, INT32_MAX}, {1, INT32_MIN}},
      {{0, INT32_MAX}, {1, INT32_MIN}});
}

TEST_F(SketchFITests, groupByVeryLowCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 0 : 1; });

  testGroupByAgg<int, int>(
      keys, values, "fi(c1, 16)", {{0, 0}, {1, 1}}, {{0, 0}, {1, 1}});
}

TEST_F(SketchFITests, groupByAllNulls) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : 30; }, nullEvery(2));

  std::unordered_map<int32_t, int32_t> expectedResult = {{0, 0}, {1, 30}};
  auto expected = toRowVector(expectedResult);
  auto op = PlanBuilder()
                .values({makeRowVector({keys, values})})
                .singleAggregation({"c0"}, {"fi(c1, 16)"})
                .planNode();
  assertQuery(op, expected);

  op = PlanBuilder()
           .values({makeRowVector({keys, values})})
           .partialAggregation({"c0"}, {"fi(c1, 16)"})
           .finalAggregation()
           .planNode();
  assertQuery(op, expected);
}

TEST_F(SketchFITests, groupByMultiple) {
  vector_size_t size = 1'000;
  auto keys1 = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto keys2 = makeFlatVector<int32_t>(size, [](auto row) { return row % 3; });
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row % 6; });

  auto plan = PlanBuilder()
                  .values({makeRowVector({keys1, keys2, values})})
                  .partialAggregation({"c0", "c1"}, {"fi(c2, 16)"})
                  .finalAggregation()
                  .planNode();

  CursorParameters params;
  params.planNode = plan;
  auto result = readCursor(params, [](Task*) {});
  auto actual = result.second;

  auto col3 = actual[0]->childAt(2)->template as<FlatVector<int32_t>>();
  EXPECT_EQ(col3->size(), 2 * 3);
  for (int i = 0; i < col3->size(); i++) {
    EXPECT_EQ(col3->valueAt(i), i);
  }

  result.first->task();
}

} // namespace bytedance::bolt::aggregate::sketches::test
