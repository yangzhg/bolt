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

#include <bolt/vector/VectorStream.h>
#include <gtest/gtest.h>
namespace bytedance::bolt::test {

class MockVectorSerde : public VectorSerde {
 public:
  MockVectorSerde() : VectorSerde(VectorSerde::Kind::kPresto) {}

  void estimateSerializedSize(
      VectorPtr vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes) override {}

  std::unique_ptr<VectorSerializer> createSerializer(
      RowTypePtr type,
      int32_t numRows,
      StreamArena* streamArena,
      const Options* options = nullptr) override {
    return nullptr;
  };

  void deserialize(
      ByteInputStream* source,
      bolt::memory::MemoryPool* pool,
      RowTypePtr type,
      RowVectorPtr* result,
      const Options* options = nullptr) override {}
};

TEST(VectorStreamTest, serdeRegistration) {
  deregisterVectorSerde();

  // Nothing registered yet.
  EXPECT_FALSE(isRegisteredVectorSerde());
  EXPECT_THROW(getVectorSerde(), BoltRuntimeError);

  // Register a mock serde.
  registerVectorSerde(std::make_unique<MockVectorSerde>());

  EXPECT_TRUE(isRegisteredVectorSerde());
  auto serde = getVectorSerde();
  EXPECT_NE(serde, nullptr);
  EXPECT_NE(dynamic_cast<MockVectorSerde*>(serde), nullptr);

  // Can't double register.
  EXPECT_THROW(
      registerVectorSerde(std::make_unique<MockVectorSerde>()),
      BoltRuntimeError);

  deregisterVectorSerde();
  EXPECT_FALSE(isRegisteredVectorSerde());
}

TEST(VectorStreamTest, namedSerdeRegistration) {
  const VectorSerde::Kind kind = VectorSerde::Kind::kPresto;

  // Nothing registered yet.
  deregisterNamedVectorSerde(kind);
  EXPECT_FALSE(isRegisteredNamedVectorSerde(kind));
  EXPECT_THROW(getNamedVectorSerde(kind), BoltRuntimeError);

  // Register a mock serde.
  registerNamedVectorSerde(kind, std::make_unique<MockVectorSerde>());

  auto serde = getNamedVectorSerde(kind);
  EXPECT_NE(serde, nullptr);
  EXPECT_NE(dynamic_cast<MockVectorSerde*>(serde), nullptr);

  const VectorSerde::Kind otherKind = VectorSerde::Kind::kUnsafeRow;
  EXPECT_FALSE(isRegisteredNamedVectorSerde(otherKind));
  EXPECT_THROW(getNamedVectorSerde(otherKind), BoltRuntimeError);

  // Can't double register.
  EXPECT_THROW(
      registerNamedVectorSerde(kind, std::make_unique<MockVectorSerde>()),
      BoltRuntimeError);

  // Register another one.
  EXPECT_FALSE(isRegisteredNamedVectorSerde(otherKind));
  EXPECT_THROW(getNamedVectorSerde(otherKind), BoltRuntimeError);
  registerNamedVectorSerde(otherKind, std::make_unique<MockVectorSerde>());
  EXPECT_TRUE(isRegisteredNamedVectorSerde(otherKind));

  deregisterNamedVectorSerde(otherKind);
  EXPECT_FALSE(isRegisteredNamedVectorSerde(otherKind));
}

} // namespace bytedance::bolt::test
