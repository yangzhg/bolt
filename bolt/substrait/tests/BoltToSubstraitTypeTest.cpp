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

#include "bolt/substrait/BoltToSubstraitType.h"
#include "bolt/substrait/SubstraitParser.h"
#include "bolt/substrait/TypeUtils.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::substrait;
namespace bytedance::bolt::substrait::test {

class BoltToSubstraitTypeTest : public ::testing::Test {
 protected:
  void testTypeConversion(const TypePtr& type) {
    SCOPED_TRACE(type->toString());

    google::protobuf::Arena arena;
    auto substraitType = typeConvertor_->toSubstraitType(arena, type);
    auto sameType = substraitParser_->parseType(substraitType);
    ASSERT_TRUE(sameType->kindEquals(type))
        << "Expected: " << type->toString()
        << ", but got: " << sameType->toString();
  }

  std::shared_ptr<BoltToSubstraitTypeConvertor> typeConvertor_;

  std::shared_ptr<SubstraitParser> substraitParser_ =
      std::make_shared<SubstraitParser>();
};

TEST_F(BoltToSubstraitTypeTest, basic) {
  testTypeConversion(BOOLEAN());

  testTypeConversion(TINYINT());
  testTypeConversion(SMALLINT());
  testTypeConversion(INTEGER());
  testTypeConversion(BIGINT());

  testTypeConversion(REAL());
  testTypeConversion(DOUBLE());

  testTypeConversion(VARCHAR());
  testTypeConversion(VARBINARY());

  testTypeConversion(ARRAY(BIGINT()));
  testTypeConversion(MAP(BIGINT(), DOUBLE()));

  testTypeConversion(ROW({"a", "b", "c"}, {BIGINT(), BOOLEAN(), VARCHAR()}));
  testTypeConversion(
      ROW({"a", "b", "c"},
          {BIGINT(), ROW({"x", "y"}, {BOOLEAN(), VARCHAR()}), REAL()}));
  testTypeConversion(ROW({}, {}));
}
} // namespace bytedance::bolt::substrait::test
