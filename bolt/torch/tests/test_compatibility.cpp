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

#include <memory>

#include <gtest/gtest.h>

#include "bolt/exec/tests/utils/QueryAssertions.h"
#include "bolt/torch/Compatibility.h"
#include "bolt/torch/tests/utils.hpp"

using namespace ::bytedance::bolt;
using ::bytedance::bolt::exec::test::assertEqualResults;

struct TorchTest : public ::bytedance::bolt::torch::testing::TorchTest,
                   ::testing::Test {
  static void SetUpTestCase() {
    ::bytedance::bolt::torch::testing::TorchTest::SetUpTestCase();
  }
};

TEST_F(TorchTest, TestCompatibility_TensorRowVectorTensor) {
  RowVectorPtr table;
  auto tensor = MakeTensor();
  auto type = bytedance::bolt::torch::TypeCast(tensor.get().dtype());
  auto table_type = ROW(std::vector<TypePtr>(tensor.get().size(1), type));
  ASSERT_NO_THROW(
      table = bytedance::bolt::torch::TorchTensorToBoltRowVector(
          pool_.get(), table_type)(tensor.get()));
  auto tensor_back = bytedance::bolt::torch::BoltRowVectorToTorchTensor(
      pool_.get(), type)(*table);
  ASSERT_TRUE(tensor.get().equal(tensor_back.get()));
}

TEST_F(TorchTest, TestCompatibility_RowVectorTensorRowVector) {
  RowVectorPtr table_back, table = MakeTable();
  auto type = asRowType(table->type())->children()[0];
  auto tensor = bytedance::bolt::torch::BoltRowVectorToTorchTensor(
      pool_.get(), type)(*table);
  table_back = bytedance::bolt::torch::TorchTensorToBoltRowVector(
      pool_.get(), asRowType(table->type()))(tensor.get());
  ASSERT_TRUE(assertEqualResults({table}, {table_back}));
}

TEST_F(TorchTest, TestCompatibility_LazyRowVectorTensorRowVector) {
  RowVectorPtr table_back, table = MakeLazyTable(1);
  auto type = asRowType(table->type())->children()[0];
  auto tensor = bytedance::bolt::torch::BoltRowVectorToTorchTensor(
      pool_.get(), type)(*table);
  table_back = bytedance::bolt::torch::TorchTensorToBoltRowVector(
      pool_.get(), asRowType(table->type()))(tensor.get());
  ASSERT_TRUE(assertEqualResults({table}, {table_back}));
}
