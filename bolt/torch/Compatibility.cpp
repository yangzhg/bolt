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

#include <c10/core/DispatchKey.h>
#include <c10/core/DispatchKeySet.h>
#include <c10/core/Storage.h>
#include <c10/core/TensorImpl.h>
#include <fmt/format.h>

#include "bolt/buffer/Buffer.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/torch/Compatibility.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/FlatVector.h"

using namespace ::bytedance::bolt;
using ::bytedance::bolt::memory::MemoryPool;

namespace bytedance::bolt::torch {

::c10::ScalarType TypeCast(TypeKind type) {
  switch (type) {
    case TypeKind::BOOLEAN:
      return ::c10::ScalarType::Bool;
    case TypeKind::SMALLINT:
      return ::c10::ScalarType::Short;
    case TypeKind::INTEGER:
      return ::c10::ScalarType::Int;
    case TypeKind::BIGINT:
      return ::c10::ScalarType::Long;
    case TypeKind::REAL:
      return ::c10::ScalarType::Float;
    case TypeKind::DOUBLE:
      return ::c10::ScalarType::Double;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
    case TypeKind::HUGEINT:
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
    case TypeKind::UNKNOWN:
    case TypeKind::FUNCTION:
    case TypeKind::OPAQUE:
    case TypeKind::TINYINT:
    case TypeKind::INVALID:
    default:
      BOLT_FAIL(fmt::format(
          "Invalid conversion from bolt TypeKind ({}) to torch ScalarType",
          ::bytedance::bolt::mapTypeKindToName(type)));
  }
}

caffe2::TypeMeta TypeCast(const TypePtr& type) {
  if (type->isShortDecimal()) {
    const auto& decimal = type->asShortDecimal();
  }
  if (!type->isPrimitiveType()) {
    BOLT_FAIL(fmt::format(
        "Cannot convert non-primitive bolt type {} to a torch type.",
        type->name()));
  }
  auto scalar_type = TypeCast(type->kind());
  caffe2::TypeMeta torch_type;
  torch_type = scalar_type;
  return torch_type;
}

TypePtr TypeCast(const caffe2::TypeMeta& type) {
  // This will throw if `type` is not a scalar type.
  auto scalar = const_cast<caffe2::TypeMeta&>(type).toScalarType();

  switch (scalar) {
    case c10::ScalarType::Bool: {
      return std::static_pointer_cast<TypePtr::element_type>(
          BooleanType::create());
    }
    case c10::ScalarType::Float: {
      return std::static_pointer_cast<TypePtr::element_type>(
          RealType::create());
    }
    case c10::ScalarType::Double: {
      return std::static_pointer_cast<TypePtr::element_type>(
          DoubleType::create());
    }
    case c10::ScalarType::UInt32:
    case c10::ScalarType::Int: {
      return std::static_pointer_cast<TypePtr::element_type>(
          IntegerType::create());
    }
    case c10::ScalarType::UInt16:
    case c10::ScalarType::Short: {
      return std::static_pointer_cast<TypePtr::element_type>(
          SmallintType::create());
    }
    case c10::ScalarType::UInt64:
    case c10::ScalarType::Long: {
      return std::static_pointer_cast<TypePtr::element_type>(
          BigintType::create());
    }
    case c10::ScalarType::BFloat16:
    case c10::ScalarType::Bits16:
    case c10::ScalarType::Bits1x8:
    case c10::ScalarType::Bits2x4:
    case c10::ScalarType::Bits4x2:
    case c10::ScalarType::Bits8:
    case c10::ScalarType::Byte:
    case c10::ScalarType::Char:
    case c10::ScalarType::ComplexDouble:
    case c10::ScalarType::ComplexFloat:
    case c10::ScalarType::ComplexHalf:
    case c10::ScalarType::Float8_e4m3fn:
    case c10::ScalarType::Float8_e4m3fnuz:
    case c10::ScalarType::Float8_e5m2:
    case c10::ScalarType::Float8_e5m2fnuz:
    case c10::ScalarType::Half:
    case c10::ScalarType::NumOptions:
    case c10::ScalarType::Int1:
    case c10::ScalarType::Int2:
    case c10::ScalarType::Int3:
    case c10::ScalarType::Int4:
    case c10::ScalarType::Int5:
    case c10::ScalarType::Int6:
    case c10::ScalarType::Int7:
    case c10::ScalarType::UInt1:
    case c10::ScalarType::UInt2:
    case c10::ScalarType::UInt3:
    case c10::ScalarType::UInt4:
    case c10::ScalarType::UInt5:
    case c10::ScalarType::UInt6:
    case c10::ScalarType::UInt7:
    case c10::ScalarType::QInt8:
    case c10::ScalarType::QInt32:
    case c10::ScalarType::QUInt8:
    case c10::ScalarType::QUInt2x4:
    case c10::ScalarType::QUInt4x2:
    case c10::ScalarType::Undefined:
    default:
      BOLT_FAIL(fmt::format(
          "torch type {} conversion to bolt type is not supported.",
          type.name()));
  }
}

Tensor BoltRowVectorToTorchTensor::operator()(const RowVector& table) {
  // This will throw if the table type is not compatible with tensor types.
  auto torch_type = TypeCast(type_);
  if (type_->cppSizeInBytes() != torch_type.itemsize()) {
    BOLT_FAIL(fmt::format(
        "Bolt dtype equivalent ({}) to tensor dtype ({}) do not have the same item size.",
        type_->name(),
        torch_type.name()));
  }

  // We don't support conversion of null values into a tensor value.
  // TODO, we should add a field to the protobuf message to allow null to
  // type conversion and fields with conversion values with reasonable
  // defaults.
  if (table.mayHaveNullsRecursive()) {
    BOLT_FAIL(
        "Conversion from RowVector to Tensor cannot contain nulls. mayHaveNullsRecursive() returned true.");
  }

  // If we convert an empty table, we return a default empty tensor.
  const auto& bolt_columns = table.children();
  const long int num_rows = table.size();
  if (bolt_columns.empty() || num_rows == 0) {
    return Tensor(torch_type);
  }

  // Invariant check.
  // 1. A tensor can only have one data type.
  // 2. Every element of the tensor need to have the same size.
  // We check that columns have the same size.
  const auto column_size = num_rows * type_->cppSizeInBytes();
  const long int num_columns = table.childrenSize();
  for (size_t i = 0; i < bolt_columns.size(); i++) {
    const auto& column = *bolt_columns[i];
    if (!column.type()->equivalent(*type_)) {
      BOLT_FAIL(fmt::format(
          "Cannot convert bolt RowVector with children of different types to "
          "tensor of one type. children[0]->type(): {}, other children type: {}.",
          type_->name(),
          column.type()->name()));
    }
    if (column.size() != num_rows) {
      BOLT_FAIL(fmt::format(
          "RowVector needs to have all of its columns of the same size to be "
          "converted to a tensor."
          "column[0]->size(): {}, other column size: {}.",
          num_rows,
          column.size()));
    }
  }

  // Make a contiguous storage large enough to store the columns.
  // And create uninitialized tensor.
  const size_t torch_storage_size = column_size * num_columns;
  void* ptr = pool_->allocate(torch_storage_size);
  char* memory = reinterpret_cast<char*>(ptr);
  if (memory == nullptr) {
    BOLT_MEM_ALLOC_ERROR(fmt::format(
        "Tensor allocation of {} bytes failed.", torch_storage_size));
  }

  // Materialize table in memory.
  [[maybe_unused]] auto _ = table.loadedVector();

  // Copy columns to the storage.
  for (auto c = bolt_columns.cbegin(); c != bolt_columns.cend(); ++c) {
    const auto& column = *c->get()->loadedVector();
    std::memcpy(memory, column.values()->asMutable<char>(), column_size);
    memory += column_size;
  }

  // Build tensor.
  return Tensor(
      ptr,
      torch_storage_size,
      torch_type,
      ::c10::IntArrayRef{num_rows, num_columns},
      pool_);
}

template <TypeKind kind>
char* getFlatBufferRawValues(VectorPtr ptr) {
  using T = typename ::bytedance::bolt::TypeTraits<kind>::NativeType;
  return reinterpret_cast<char*>(ptr->asFlatVector<T>()->mutableRawValues());
}

std::shared_ptr<RowVector> TorchTensorToBoltRowVector::operator()(
    const ::at::Tensor& tensor) {
  if (tensor.dim() != 2) {
    BOLT_FAIL(fmt::format(
        "Only tensors of 2 dimensions are supported. Tensor dimensions: {}",
        tensor.dim()));
  }
  if (!tensor.is_cpu()) {
    BOLT_FAIL(fmt::format("Non CPU tensors are not supported."));
  }

  const auto rowOutputType = asRowType(type_);
  const auto dtype = TypeCast(tensor.dtype());
  const auto element_size = tensor.dtype().itemsize();
  const auto num_columns = tensor.size(1);
  const auto num_rows = tensor.size(0);
  const auto row_stride = tensor.stride(0);
  const auto col_stride = tensor.stride(1);
  const auto col_size = col_stride * element_size;
  const auto* tensor_data = static_cast<const char*>(tensor.const_data_ptr()) +
      tensor.storage_offset();

  if (dtype->cppSizeInBytes() != tensor.dtype().itemsize()) {
    BOLT_FAIL(fmt::format(
        "Bolt dtype equivalent ({}) to tensor dtype ({}) do not have the same item size: {}, {}.",
        dtype->name(),
        tensor.dtype().name(),
        dtype->cppSizeInBytes(),
        tensor.dtype().itemsize()));
  }

  // Check that output row type and tensor type are equal.
  if (num_columns != rowOutputType->children().size()) {
    BOLT_FAIL(fmt::format(
        "Tensor number of columns ({}) and output type number of children ({}) do not match.",
        num_columns,
        rowOutputType->children().size()));
  }
  for (const auto& child : rowOutputType->children()) {
    if (*child != *dtype) {
      BOLT_FAIL(fmt::format(
          "Torch operator output type ({}) does not match tensor data type ({}).",
          child->toString(),
          dtype->toString()));
    }
  }

  // Allocate space for each column.
  std::vector<VectorPtr> columns;
  columns.reserve(num_columns);
  for (size_t i = 0; i < num_columns; i++) {
    columns.emplace_back(
        BaseVector::create<FlatVector<int8_t>>(dtype, num_rows, pool_));
  }

  // Perform copy.
  // This needs to be tested with several types of tensors.
  // We might need it to call tensor.to_dense().
  // If the the storage is row major, we can use memcpy on individual columns.
  // https://discuss.pytorch.org/t/are-tensors-column-major-or-row-major/832
  // The link above reports that storage is row major.
  if (row_stride == 1) {
    for (size_t i = 0; i < num_columns; i++) {
      const auto col_offset = i * col_size;
      const auto* torch_column = tensor_data + col_offset;
      TypeKind typeKind = rowOutputType->childAt(i)->kind();
      char* data = BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          getFlatBufferRawValues, typeKind, columns[i]);
      assert(col_size == columns[i]->usedSize());
      std::memcpy(data, torch_column, col_size);
    }
  }
  // Otherwise, we need to copy element by element.
  else {
    for (size_t i = 0; i < num_rows; i++) {
      for (size_t j = 0; j < num_columns; j++) {
        const auto offset =
            (tensor.stride(0) * i + tensor.stride(1) * j) * element_size;
        TypeKind typeKind = rowOutputType->childAt(j)->kind();
        char* data = BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
            getFlatBufferRawValues, typeKind, columns[j]);
        const auto* torch_element = tensor_data + offset;
        memcpy(data + element_size * i, torch_element, element_size);
      }
    }
  }

  return std::make_shared<RowVector>(
      pool_, type_, BufferPtr{nullptr}, num_rows, std::move(columns));
}
} // namespace bytedance::bolt::torch
