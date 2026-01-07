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

#include <gtest/gtest.h>
#include "bolt/dwio/paimon/deletionvectors/DeletionVector.h"
#include "roaring/roaring.hh"
namespace bytedance::bolt::paimon {

class DeleteionVectorTest : public testing::Test {
 public:
  class TestDeleteionVector : public DeletionVector {
   public:
    TestDeleteionVector(roaring::Roaring bitmap, bolt::memory::MemoryPool* pool)
        : DeletionVector(std::move(bitmap), pool){};
  };

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    rootPool_ = memory::memoryManager()->addRootPool("DeleteionVectorTest");
    leafPool_ = rootPool_->addLeafChild("DeleteionVectorTest");
  }

  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
};

TEST_F(DeleteionVectorTest, normal) {
  TestDeleteionVector testDeleteionVector(
      roaring::Roaring{5, 10, 12, 16, 17, 50, 52, 55, 100}, leafPool_.get());
  ASSERT_EQ(0, testDeleteionVector.getLastValue());

  BufferPtr deleteBuffer;
  testDeleteionVector.getDeletionVector(0, 10, &deleteBuffer);
  ASSERT_GE(deleteBuffer->size(), bits::nbytes(10));
  ASSERT_EQ(5, testDeleteionVector.getLastValue());
  ASSERT_TRUE(bits::isBitSet(deleteBuffer->as<uint8_t>(), 5));
  ASSERT_EQ(1, bits::countBits(deleteBuffer->as<uint64_t>(), 0, 10));

  testDeleteionVector.getDeletionVector(10, 30, &deleteBuffer);
  ASSERT_GE(deleteBuffer->size(), bits::nbytes(30));
  ASSERT_EQ(17, testDeleteionVector.getLastValue());
  ASSERT_TRUE(bits::isBitSet(deleteBuffer->as<uint8_t>(), 0));
  ASSERT_TRUE(bits::isBitSet(deleteBuffer->as<uint8_t>(), 2));
  ASSERT_TRUE(bits::isBitSet(deleteBuffer->as<uint8_t>(), 6));
  ASSERT_TRUE(bits::isBitSet(deleteBuffer->as<uint8_t>(), 7));
  ASSERT_EQ(4, bits::countBits(deleteBuffer->as<uint64_t>(), 0, 10));

  testDeleteionVector.getDeletionVector(10, 1, &deleteBuffer);
  ASSERT_GE(deleteBuffer->size(), bits::nbytes(1));
  ASSERT_EQ(10, testDeleteionVector.getLastValue());
  ASSERT_TRUE(bits::isBitSet(deleteBuffer->as<uint8_t>(), 0));
  ASSERT_EQ(1, bits::countBits(deleteBuffer->as<uint64_t>(), 0, 10));

  testDeleteionVector.getDeletionVector(100, 10, &deleteBuffer);
  ASSERT_GE(deleteBuffer->size(), bits::nbytes(10));
  ASSERT_EQ(100, testDeleteionVector.getLastValue());
  ASSERT_TRUE(bits::isBitSet(deleteBuffer->as<uint8_t>(), 0));
  ASSERT_EQ(1, bits::countBits(deleteBuffer->as<uint64_t>(), 0, 10));

  testDeleteionVector.getDeletionVector(200, 100, &deleteBuffer);
  ASSERT_GE(deleteBuffer->size(), bits::nbytes(100));
  ASSERT_EQ(100, testDeleteionVector.getLastValue());
  ASSERT_EQ(0, bits::countBits(deleteBuffer->as<uint64_t>(), 0, 10));
};
} // namespace bytedance::bolt::paimon