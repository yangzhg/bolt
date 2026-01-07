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

#include <fmt/core.h>
#include <gtest/gtest.h>
#include <algorithm>

#include <folly/Conv.h>
#include <folly/Optional.h>

#include "bolt/buffer/Buffer.h"
#include "bolt/vector/VectorUtil.h"
namespace bytedance {
namespace bolt {
namespace test {

template <typename T>
class VectorUtilTest : public testing::Test {
 public:
  void runTest(folly::Optional<int32_t> size = folly::none) {
    std::vector<T> vec;
    constexpr int32_t kSize = 3;
    for (size_t i = 1; i <= kSize; ++i) {
      vec.emplace_back(folly::to<T>(i));
    }
    auto buffer = copyToBuffer(vec, pool_.get(), size);
    int32_t actualBufferSize =
        size.hasValue() ? std::min(size.value(), kSize) : kSize;
    ASSERT_EQ(actualBufferSize * sizeof(T), buffer->size());
  }

  void runTestWithEmptyVector() {
    std::vector<T> vec;
    auto buffer = copyToBuffer(vec, pool_.get());
    ASSERT_EQ(0, buffer->size());
  }

  void runTestWithEmptyVectorAndReturnsNullptr() {
    std::vector<T> vec;
    auto buffer = copyToBuffer(
        vec, pool_.get(), folly::none /*size*/, true /*returnsNullptr*/);
    ASSERT_EQ(nullptr, buffer);
  }

 protected:
  void SetUp() override {
    pool_ = memoryManager_.addLeafPool("VectorUtilTest");
  }

  memory::MemoryManager memoryManager_;
  std::shared_ptr<memory::MemoryPool> pool_;
};

using ScalarTypes =
    ::testing::Types<int8_t, int16_t, int32_t, int64_t, double, char>;

TYPED_TEST_CASE(VectorUtilTest, ScalarTypes);

TYPED_TEST(VectorUtilTest, copyToBuffer) {
  this->runTest();
}

TYPED_TEST(VectorUtilTest, copyToBufferWithSpecifiedSize) {
  this->runTest(1 /*size*/);
}

TYPED_TEST(VectorUtilTest, copyToBufferWithEmptyVector) {
  this->runTestWithEmptyVector();
}

TYPED_TEST(VectorUtilTest, copyToBufferWithEmptyVectorReturnsNullptr) {
  this->runTestWithEmptyVectorAndReturnsNullptr();
}
} // namespace test
} // namespace bolt
} // namespace bytedance
