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
namespace bytedance::bolt::functions {
namespace {

class SplitToMapTest : public test::FunctionBaseTest {};

TEST_F(SplitToMapTest, basic) {
  auto data = makeRowVector({
      makeFlatVector<std::string>({
          "1:10,2:20,3:30,4:",
          "4:40",
          "",
          ":00,1:11,3:33,5:55,7:77",
      }),
  });

  auto result = evaluate("split_to_map(c0, ',', ':')", data);

  auto expected = makeMapVector<std::string, std::string>({
      {{"1", "10"}, {"2", "20"}, {"3", "30"}, {"4", ""}},
      {{"4", "40"}},
      {},
      {{"", "00"}, {"1", "11"}, {"3", "33"}, {"5", "55"}, {"7", "77"}},
  });

  bolt::test::assertEqualVectors(expected, result);
}

TEST_F(SplitToMapTest, invalidInput) {
  auto data = makeRowVector({
      makeFlatVector<std::string>({
          "1:10,2:20,1:30",
      }),
  });

  auto splitToMap = [&](const std::string& entryDelimiter,
                        const std::string& keyValueDelimiter) {
    evaluate(
        fmt::format(
            "split_to_map(c0, '{}', '{}')", entryDelimiter, keyValueDelimiter),
        data);
  };

  BOLT_ASSERT_THROW(
      splitToMap(".", "."),
      "entryDelimiter and keyValueDelimiter must not be the same");
  BOLT_ASSERT_THROW(splitToMap(".", ""), "keyValueDelimiter is empty");
  BOLT_ASSERT_THROW(splitToMap("", "."), "entryDelimiter is empty");
  BOLT_ASSERT_THROW(
      splitToMap(":", ","),
      "Key-value delimiter must appear exactly once in each entry. Bad input: '1'");
  BOLT_ASSERT_THROW(
      splitToMap(",", ":"), "Duplicate keys (1) are not allowed.");
}

} // namespace
} // namespace bytedance::bolt::functions
