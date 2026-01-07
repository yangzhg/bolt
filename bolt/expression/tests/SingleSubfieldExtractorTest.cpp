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

#include "bolt/expression/SingleSubfieldExtractor.h"
#include <gtest/gtest.h>
#include <memory>
#include <typeinfo>
#include "bolt/core/Expressions.h"
#include "bolt/type/Type.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;

class SingleSubfieldExtractorTest : public testing::Test {
 protected:
  // Helper method for FieldAccess with no input
  std::shared_ptr<const core::ITypedExpr> makeFieldAccess(
      const std::string& name,
      const TypePtr& type) {
    return std::make_shared<core::FieldAccessTypedExpr>(type, name);
  }

  // Helper method for FieldAccess with input
  std::shared_ptr<const core::ITypedExpr> makeFieldAccess(
      const std::string& name,
      const TypePtr& type,
      const std::shared_ptr<const core::ITypedExpr>& input) {
    return std::make_shared<core::FieldAccessTypedExpr>(type, input, name);
  }

  std::shared_ptr<const core::ITypedExpr> makeDereference(
      const std::string& name,
      const TypePtr& type,
      std::shared_ptr<const core::ITypedExpr> base) {
    // Find the index of the field in the base type
    auto rowType = base->type()->asRow();
    auto index = rowType.getChildIdx(name);
    return std::make_shared<core::DereferenceTypedExpr>(type, base, index);
  }

  std::shared_ptr<const core::ITypedExpr> makeCast(
      const TypePtr& type,
      std::shared_ptr<const core::ITypedExpr> input) {
    return std::make_shared<core::CastTypedExpr>(type, input, false);
  }

  std::shared_ptr<const core::ITypedExpr> makeCall(
      const std::string& name,
      const TypePtr& type,
      std::vector<std::shared_ptr<const core::ITypedExpr>> inputs) {
    return std::make_shared<core::CallTypedExpr>(type, std::move(inputs), name);
  }
};

TEST_F(SingleSubfieldExtractorTest, invalidDereference) {
  // This test verifies that DereferenceTypedExpr requires a ROW type input
  auto varcharType = VARCHAR();

  // Try to create a dereference from a non-ROW type (should fail)
  auto baseField = makeFieldAccess("a", varcharType);
  EXPECT_THROW(makeDereference("b", varcharType, baseField), std::bad_cast);
}

TEST_F(SingleSubfieldExtractorTest, validDereference) {
  // Create a ROW type with multiple fields
  auto doubleType = DOUBLE();
  auto varcharType = VARCHAR();
  auto rowType = ROW({"a", "b"}, {doubleType, varcharType});

  // Create base field access followed by dereference
  auto baseField = makeFieldAccess("base", rowType);
  auto derefField = makeDereference("a", doubleType, baseField);

  SingleSubfieldExtractor extractor;
  auto result = extractor.extract(derefField.get());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (std::vector<std::string>{"base", "a"}));
  EXPECT_EQ(result->second, 0);
}

TEST_F(SingleSubfieldExtractorTest, simpleFieldAccess) {
  auto type = VARCHAR();
  auto expr = makeFieldAccess("name", type);

  SingleSubfieldExtractor extractor;
  auto result = extractor.extract(expr.get());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, std::vector<std::string>{"name"});
  EXPECT_EQ(result->second, 0);
}

TEST_F(SingleSubfieldExtractorTest, nestedFieldAccess) {
  auto type = VARCHAR();
  auto inner = makeFieldAccess("inner", type);
  auto outer = makeFieldAccess("outer", type, {inner});

  SingleSubfieldExtractor extractor;
  auto result = extractor.extract(outer.get());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (std::vector<std::string>{"inner", "outer"}));
  EXPECT_EQ(result->second, 0);
}

TEST_F(SingleSubfieldExtractorTest, castExpression) {
  auto varchar = VARCHAR();
  auto integer = INTEGER();
  auto field = makeFieldAccess("field", varchar);
  auto cast = makeCast(integer, field);

  SingleSubfieldExtractor extractor;
  auto result = extractor.extract(cast.get());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, std::vector<std::string>{"field"});
  EXPECT_EQ(result->second, 1);
}

TEST_F(SingleSubfieldExtractorTest, callSingleChain) {
  auto type = VARCHAR();
  auto field = makeFieldAccess("field", type);
  auto call = makeCall("someFunction", type, {field});

  SingleSubfieldExtractor extractor;
  auto result = extractor.extract(call.get());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, std::vector<std::string>{"field"});
  EXPECT_EQ(result->second, 1);
}

TEST_F(SingleSubfieldExtractorTest, callMultipleChains) {
  auto type = VARCHAR();
  auto field1 = makeFieldAccess("field1", type);
  auto field2 = makeFieldAccess("field2", type);
  auto call = makeCall("someFunction", type, {field1, field2});

  SingleSubfieldExtractor extractor;
  auto result = extractor.extract(call.get());

  ASSERT_FALSE(result.has_value());
}

TEST_F(SingleSubfieldExtractorTest, nullExpression) {
  SingleSubfieldExtractor extractor;
  auto result = extractor.extract(nullptr);

  ASSERT_FALSE(result.has_value());
}

TEST_F(SingleSubfieldExtractorTest, fieldTypedExpr) {
  auto type = VARCHAR();
  auto inner = makeFieldAccess("inner", type);
  auto outer = makeFieldAccess("outer", type, {inner});

  SingleSubfieldExtractor extractor;
  auto [expr, depth] = extractor.fieldTypedExpr(outer.get());

  ASSERT_NE(expr, nullptr);
  EXPECT_EQ(expr->typedExprKind(), core::TypedExprKind::kFieldAccess);
  EXPECT_EQ(
      static_cast<const core::FieldAccessTypedExpr*>(expr)->name(), "outer");
  EXPECT_EQ(depth, 0);
}

TEST_F(SingleSubfieldExtractorTest, nestedRowType) {
  // Create ROW(b DOUBLE) type
  auto doubleType = DOUBLE();
  auto rowType = ROW({"b"}, {doubleType});

  // Create expression tree: a.b where 'a' is ROW(b DOUBLE)
  auto baseField = makeFieldAccess("a", rowType);
  auto nestedField = makeFieldAccess("b", doubleType, {baseField});

  SingleSubfieldExtractor extractor;
  auto result = extractor.extract(nestedField.get());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (std::vector<std::string>{"a", "b"}));
  EXPECT_EQ(result->second, 0);
}

TEST_F(SingleSubfieldExtractorTest, deeplyNestedRowType) {
  // Create ROW(c ROW(b DOUBLE)) type
  auto doubleType = DOUBLE();
  auto innerRowType = ROW({"b"}, {doubleType});
  auto outerRowType = ROW({"c"}, {innerRowType});

  // Create expression tree: a.c.b where 'a' is ROW(c ROW(b DOUBLE))
  auto baseField = makeFieldAccess("a", outerRowType);
  auto middleField = makeDereference("c", innerRowType, baseField);
  auto innerField = makeDereference("b", doubleType, middleField);

  SingleSubfieldExtractor extractor;
  auto result = extractor.extract(innerField.get());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (std::vector<std::string>{"a", "c", "b"}));
  EXPECT_EQ(result->second, 0);
}

TEST_F(SingleSubfieldExtractorTest, nestedRowTypeWithCast) {
  // Create ROW(b DOUBLE) type
  auto doubleType = DOUBLE();
  auto rowType = ROW({"b"}, {doubleType});

  // Create expression tree: CAST(a.b AS DOUBLE) where 'a' is ROW(b DOUBLE)
  auto baseField = makeFieldAccess("a", rowType);
  auto nestedField = makeFieldAccess("b", doubleType, {baseField});
  auto castExpr = makeCast(doubleType, nestedField);

  SingleSubfieldExtractor extractor;
  auto result = extractor.extract(castExpr.get());

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (std::vector<std::string>{"a", "b"}));
  EXPECT_EQ(result->second, 1);
}