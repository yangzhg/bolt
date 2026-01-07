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
#include "bolt/functions/prestosql/tests/utils/FunctionBaseTest.h"
using namespace bytedance::bolt::test;
namespace bytedance::bolt::functions {
namespace {

class MultimapFromEntriesTest : public test::FunctionBaseTest {};

TEST_F(MultimapFromEntriesTest, basic) {
  auto row = [](std::vector<variant>&& inputs) {
    return variant::row(std::move(inputs));
  };

  auto input = makeArrayOfRowVector(
      ROW({INTEGER(), VARCHAR()}),
      {
          // No duplicate keys.
          {row({1, "red"}), row({2, "blue"}), row({3, "green"})},
          // Some duplicate keys.
          {row({1, "pink"}),
           row({1, "red"}),
           row({2, "blue"}),
           row({2, variant::null(TypeKind::VARCHAR)}),
           row({2, "lightblue"}),
           row({3, "green"})},
          // Empty array.
          {},
      });

  auto result = evaluate("multimap_from_entries(c0)", makeRowVector({input}));

  auto expectedKeys = makeFlatVector<int32_t>({1, 2, 3, 1, 2, 3});
  auto expectedValues = makeNullableArrayVector<std::string>({
      {"red"},
      {"blue"},
      {"green"},
      {"pink", "red"},
      {"blue", std::nullopt, "lightblue"},
      {"green"},
  });

  auto expectedMap = makeMapVector({0, 3, 6}, expectedKeys, expectedValues);
  assertEqualVectors(expectedMap, result);
}

TEST_F(MultimapFromEntriesTest, nullKey) {
  auto row = [](std::vector<variant>&& inputs) {
    return variant::row(std::move(inputs));
  };

  auto input = makeArrayOfRowVector(
      ROW({INTEGER(), VARCHAR()}),
      {
          {
              row({1, "red"}),
              row({variant::null(TypeKind::INTEGER), "blue"}),
              row({3, "green"}),
          },
      });

  BOLT_ASSERT_THROW(
      evaluate("multimap_from_entries(c0)", makeRowVector({input})),
      "map key cannot be null");
}

} // namespace
} // namespace bytedance::bolt::functions
