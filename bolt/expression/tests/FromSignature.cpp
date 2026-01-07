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

#include "bolt/expression/SignatureBinder.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/type/Type.h"

#include <gtest/gtest.h>

using namespace ::bytedance::bolt;

TypePtr tryResolveType(const exec::TypeSignature& signature) {
  std::unordered_map<std::string, exec::SignatureVariable> variables;
  std::unordered_map<std::string, TypePtr> resolvedTypeVariables;
  return exec::SignatureBinder::tryResolveType(
      signature, variables, resolvedTypeVariables);
}

struct TypeSignatureTest : public testing::Test {
  void SetUp() override {
    ::parse::registerTypeResolver();
  }

  void TestRoundTrip(const Type& type) {
    // OPAQUE and INVALID types are not supported.
    switch (type.kind()) {
      case TypeKind::OPAQUE:
      case TypeKind::INVALID:
        ASSERT_ANY_THROW(auto _ = exec::TypeSignature(type));
        return;
      default:
        break;
    }

    exec::TypeSignature signature(type);
    std::string err_msg = fmt::format(
        "type: {}\nsignature: {}", type.toString(), signature.toString());

    TypePtr rhs = tryResolveType(signature);
    ASSERT_NE(rhs, nullptr) << err_msg;

    exec::TypeSignature rhs_signature(*rhs);
    err_msg += fmt::format(
        "\nsignature type: {}\n signature type signature: {}",
        rhs->toString(),
        rhs_signature.toString());

    // Use standard type comparison.
    ASSERT_EQ(type, *rhs) << err_msg;
  }
};

TEST_F(TypeSignatureTest, SignatureFromUnknownType) {
  TestRoundTrip(UnknownType());
}

TEST_F(TypeSignatureTest, SignatureFromScalarType) {
  TestRoundTrip(*TypeFactory<TypeKind::BIGINT>::create());
}

TEST_F(TypeSignatureTest, SignatureFromDecimalType) {
  TestRoundTrip(ShortDecimalType(16, 1));
  TestRoundTrip(LongDecimalType(19, 1));
}

TEST_F(TypeSignatureTest, SignatureFromIntervalType) {
  TestRoundTrip(*INTERVAL_YEAR_MONTH());
  TestRoundTrip(*INTERVAL_DAY_TIME());
}

TEST_F(TypeSignatureTest, SignatureFromDateType) {
  TestRoundTrip(*DATE());
}

TEST_F(TypeSignatureTest, SignatureFromArrayType) {
  TestRoundTrip(*ARRAY(TypeFactory<TypeKind::BIGINT>::create()));
}

TEST_F(TypeSignatureTest, SignatureFromMapType) {
  TestRoundTrip(*MAP(
      TypeFactory<TypeKind::BIGINT>::create(),
      TypeFactory<TypeKind::BIGINT>::create()));
}

TEST_F(TypeSignatureTest, SignatureFromRowType) {
  TestRoundTrip(*ROW({std::make_pair(
      "row_type",
      ROW({"int_type"}, {TypeFactory<TypeKind::BIGINT>::create()}))}));
}
