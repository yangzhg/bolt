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

#pragma once

#include <gtest/gtest.h>
#include <memory>

#include "bolt/common/memory/Memory.h"
#include "bolt/exec/Operator.h"
#include "bolt/torch/PlanNode.h"
#include "bolt/torch/Tensor.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

namespace bytedance::bolt::torch::testing {
using namespace ::bytedance::bolt;
using BoltType = TypePtr;
using TorchType = ::caffe2::TypeMeta;
using TorchTensor = ::at::Tensor;
using BoltTensor = ::bytedance::bolt::torch::Tensor;

std::string_view AsStringView(const VectorPtr vec);

static inline constexpr auto kMultiplyByTwoTorchScript = R"JIT(
  def multiply_by_two(tensor):
    return tensor * 2
  )JIT";

RowVectorPtr MultiplyByTwo(const RowVectorPtr input);

struct TorchTest {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  /// Make a default tensor.
  BoltTensor MakeTensor(
      ::at::IntArrayRef sizes = {64 /* num_rows */, 32 /* num_columns */},
      TorchType type = TorchType::Make<float>());

  /// Make a default table. The table values are different than those of
  /// the default tensor.
  RowVectorPtr MakeTable(
      const size_t num_columns = 64,
      const size_t num_rows = 32,
      BoltType type = TypeFactory<TypeKind::REAL>::create());

  /// Make a default table that is not materialized in memory.
  /// See also: `MakeTable()`
  RowVectorPtr MakeLazyTable(
      const size_t num_columns = 64,
      const size_t num_rows = 32,
      BoltType type = TypeFactory<TypeKind::REAL>::create());

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  ::bytedance::bolt::test::VectorMaker vector_maker_{pool_.get()};
};
} // namespace bytedance::bolt::torch::testing
