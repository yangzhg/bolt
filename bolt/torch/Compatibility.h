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

#include "bolt/torch/Tensor.h"
#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::torch {

/// Conversion operator from libtorch type to bolt type.
///
/// For now we only support primitive types.
/// Types that cannot be stored in a tensor should be converted in different
/// operator. For instance, computation of embeddings (mapping tokens to
/// integers) should be done in another operator.
///
/// Implementation of more complex types such as tensor quantized types require
/// a lot more implementation and are not supported yet.
///
/// This function will throw a std::invalid_argument if the input type cannot be
/// matched with a bolt type.
::bytedance::bolt::TypePtr TypeCast(const ::caffe2::TypeMeta& type);
::bytedance::bolt::TypeKind TypeCast(::c10::ScalarType type);

/// Conversion operator from bolt type to libtorch type.
///
/// See also `bytedance::bolt::TypePtr TypeCast(const caffe2::TypeMeta& type);`
::caffe2::TypeMeta TypeCast(const ::bytedance::bolt::TypePtr&);

/// Functors to convert torch module data formats into bolt data formats.
/// The intent is to support bolt operators input/output in and out of
/// torch modules.
///
/// ::torch::jit::IValue() <- ::c10::IValue (ATen/core/ivalue.h)
/// is a union over multiple torch types. We aim to support the `::at::Tensor`
/// type for now. More specifically, we restrict tensors to 2D tensor with
/// a straight mapping to a table (a RowVector in bolt parlance).
template <class Input, class Output>
class CopyConverter {
 public:
  Output operator()(const Input&);
};

/// Converter from libtorch Tensor output to bolt Operator output.
/// The functor checks that the tensor type has a compatible with bolt types,
/// and that the input has the right number of dimensions (2, e.g flat column
/// vectors).
/// At the moment, the tensor is assumed to be a dense tensor.
class TorchTensorToBoltRowVector
    : public CopyConverter<
          Tensor,
          std::shared_ptr<::bytedance::bolt::RowVector>> {
 public:
  explicit TorchTensorToBoltRowVector(
      ::bytedance::bolt::memory::MemoryPool* pool,
      ::bytedance::bolt::TypePtr type)
      : pool_(pool), type_(std::move(type)) {}

  std::shared_ptr<::bytedance::bolt::RowVector> operator()(const ::at::Tensor&);

 private:
  bytedance::bolt::memory::MemoryPool* pool_;
  const ::bytedance::bolt::TypePtr type_;
};

/// Converter from bolt Operator input to libtorch input Tensor.
/// The functor checks that the bolt type is compatible with the Tensor
/// supported types and that the input has the right number of dimensions
/// (2, e.g flat column vectors) and that a single data type across values.
/// Null values are not supported but the expensive check is skipped.
class BoltRowVectorToTorchTensor : public CopyConverter<
                                       ::bytedance::bolt::RowVector,
                                       ::bytedance::bolt::torch::Tensor> {
 public:
  BoltRowVectorToTorchTensor(
      ::bytedance::bolt::memory::MemoryPool* pool,
      bytedance::bolt::TypePtr type)
      : pool_(std::move(pool)), type_(std::move(type)) {}

  ::bytedance::bolt::torch::Tensor operator()(
      const ::bytedance::bolt::RowVector&);

 private:
  ::bytedance::bolt::memory::MemoryPool* pool_;
  const ::bytedance::bolt::TypePtr type_;
};
} // namespace bytedance::bolt::torch
