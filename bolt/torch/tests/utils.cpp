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
#include <vector>

#include <ATen/core/TensorBody.h>

#include "bolt/torch/Compatibility.h"
#include "bolt/torch/tests/utils.hpp"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/LazyVector.h"

using namespace ::bytedance::bolt;

namespace {
std::string UniqueNodeId() {
  static std::atomic_uint64_t ids_;
  return fmt::format("TorchTestNode[{}]", ids_.fetch_add(1));
}

template <TypeKind kind>
char* getFlatBufferRawValues(BaseVector* ptr) {
  using T = typename ::bytedance::bolt::TypeTraits<kind>::NativeType;
  return reinterpret_cast<char*>(ptr->asFlatVector<T>()->mutableRawValues());
}

template <TypeKind kind>
VectorPtr MakeLazyVector(
    bytedance::bolt::test::VectorMaker& vector_maker,
    size_t size) {
  using T = typename ::bytedance::bolt::TypeTraits<kind>::NativeType;
  return vector_maker.lazyFlatVector<T>(
      size, []([[maybe_unused]] auto i) { return T(); });
}
} // namespace

namespace bytedance::bolt::torch::testing {

std::string_view AsStringView(const ::bytedance::bolt::VectorPtr vec) {
  auto typeKind = vec->type()->kind();
  auto* loaded_vec = vec->loadedVector();
  char* data = BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
      getFlatBufferRawValues, typeKind, loaded_vec);
  return {data, loaded_vec->usedSize()};
}

BoltTensor TorchTest::MakeTensor(::at::IntArrayRef sizes, TorchType type) {
  size_t total_size =
      std::reduce(
          sizes.cbegin(),
          sizes.cend(),
          1,
          [](size_t acc, const auto& val) { return acc * val; }) *
      type.itemsize();
  void* data = pool_->allocate(total_size);
  memset(data, 2, total_size);
  return {data, total_size, std::move(type), sizes, pool_.get()};
}

RowVectorPtr TorchTest::MakeTable(
    const size_t num_columns,
    const size_t num_rows,
    BoltType type) {
  std::vector<VectorPtr> children;
  std::vector<std::string> columns_name;
  std::vector<BoltType> columns_type(num_columns, type);
  auto typeKind = type->kind();

  for (size_t i = 0; i < num_columns; i++) {
    children.emplace_back(BaseVector::create(type, num_rows, pool_.get()));
    columns_name.emplace_back(fmt::format("test_column_{}", i));
  }

  auto row_dtype = TypeFactory<TypeKind::ROW>::create(
      std::move(columns_name), std::move(columns_type));
  return std::make_shared<RowVector>(
      pool_.get(),
      std::move(row_dtype),
      nullptr,
      num_rows,
      std::move(children));
}

RowVectorPtr TorchTest::MakeLazyTable(
    const size_t num_columns,
    const size_t num_rows,
    BoltType type) {
  std::vector<VectorPtr> children;
  std::vector<std::string> columns_name;
  std::vector<BoltType> columns_type(num_columns, type);
  auto typeKind = type->kind();

  for (size_t i = 0; i < num_columns; i++) {
    auto vector = BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
        MakeLazyVector, typeKind, vector_maker_, num_rows);
    children.emplace_back(std::move(vector));
    columns_name.emplace_back(fmt::format("test_column_{}", i));
  }

  auto row_dtype = TypeFactory<TypeKind::ROW>::create(
      std::move(columns_name), std::move(columns_type));
  return std::make_shared<RowVector>(
      pool_.get(),
      std::move(row_dtype),
      nullptr,
      num_rows,
      std::move(children));
}

namespace {
template <typename T>
void MultiplyByTwo(VectorPtr ptr) {
  auto vec = ptr->loadedVector();
  auto values =
      reinterpret_cast<T*>(vec->asFlatVector<T>()->mutableRawValues());
  for (size_t i = 0; i < vec->size(); i++) {
    values[i] *= 2;
  }
}
} // namespace

RowVectorPtr MultiplyByTwo(const RowVectorPtr input) {
  auto t = std::dynamic_pointer_cast<RowVector>(BaseVector::copy(*input));
  for (auto col : t->children()) {
    auto typeKind = col->type()->kind();
    switch (typeKind) {
      case TypeKind::BOOLEAN:
        MultiplyByTwo<TypeTraits<TypeKind::BOOLEAN>::NativeType>(col);
        break;
      case TypeKind::TINYINT:
        MultiplyByTwo<TypeTraits<TypeKind::TINYINT>::NativeType>(col);
        break;
      case TypeKind::SMALLINT:
        MultiplyByTwo<TypeTraits<TypeKind::SMALLINT>::NativeType>(col);
        break;
      case TypeKind::INTEGER:
        MultiplyByTwo<TypeTraits<TypeKind::INTEGER>::NativeType>(col);
        break;
      case TypeKind::BIGINT:
        MultiplyByTwo<TypeTraits<TypeKind::BIGINT>::NativeType>(col);
        break;
      case TypeKind::HUGEINT:
        MultiplyByTwo<TypeTraits<TypeKind::HUGEINT>::NativeType>(col);
        break;
      case TypeKind::REAL:
        MultiplyByTwo<TypeTraits<TypeKind::REAL>::NativeType>(col);
        break;
      case TypeKind::DOUBLE:
        MultiplyByTwo<TypeTraits<TypeKind::DOUBLE>::NativeType>(col);
        break;
      default:
        BOLT_FAIL(fmt::format(
            "Input table column type {}, cannot be multiplied by two.",
            col->type()->toString()));
    }
  }
  return t;
}
} // namespace bytedance::bolt::torch::testing
