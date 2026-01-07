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

#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/torch/PlanNode.h"
#include "bolt/torch/tests/utils.hpp"

using ::bytedance::bolt::exec::test::PlanBuilder;
using namespace ::bytedance::bolt;

struct TorchTest : public ::bytedance::bolt::torch::testing::TorchTest,
                   ::testing::Test {
  static void SetUpTestCase() {
    ::bytedance::bolt::torch::testing::TorchTest::SetUpTestCase();
  }
};

TEST_F(TorchTest, TestPlanNode) {
  constexpr auto kTorchScriptInput = R"JIT(
  def identity(tensor):
    return tensor
   )JIT";

  auto plan_node = PlanBuilder()
                       .values({}) // Plan node requires non null source node
                                   // otherwise serialization will attempt to
                                   // call serialize method on nullptr
                       .torch(kTorchScriptInput, ROW({}))
                       .planNode();
  auto node =
      std::dynamic_pointer_cast<const ::bytedance::bolt::torch::TorchNode>(
          plan_node);

  // Check that node is well formed.
  ASSERT_EQ(node->moduleScript(), kTorchScriptInput);
  ASSERT_FALSE(node->name().empty());

  // Check that serialization round-trip is idempotent.
  core::PlanNode::registerSerDe();
  auto serialized = node->serialize();
  auto deserialized =
      std::dynamic_pointer_cast<const ::bytedance::bolt::torch::TorchNode>(
          ::bytedance::bolt::torch::TorchNode::create(serialized, pool_.get()));

  ASSERT_NE(deserialized, nullptr);
  ASSERT_EQ(node->moduleScript(), deserialized->moduleScript());
  ASSERT_EQ(node->name(), deserialized->name());
  ASSERT_EQ(node->sources().size(), deserialized->sources().size());
  for (size_t i = 0; i < node->sources().size(); i++) {
    ASSERT_EQ(node->sources()[i]->name(), deserialized->sources()[i]->name());
  }
}
