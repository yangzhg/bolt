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

#include <cstdint>
#include <functional>
#include <numeric>

#include <torch/csrc/autograd/generated/variable_factories.h>

#include "bolt/torch/Tensor.h"

namespace bytedance::bolt::torch {
Tensor::Tensor(
    void* data,
    size_t data_size,
    ::caffe2::TypeMeta tensor_type,
    ::at::IntArrayRef tensor_sizes,
    ::bytedance::bolt::memory::MemoryPool* pool)
    : pool_(std::move(pool)), data_(data), data_size_(data_size) {
  auto options = ::at::TensorOptions().dtype(tensor_type);
  std::vector<int64_t> tensor_strides(tensor_sizes.size(), 1);
  for (std::size_t i = 1; i < tensor_sizes.size(); i++) {
    tensor_strides[i] = tensor_sizes[i - 1] * tensor_strides[i - 1];
  }
  this->tensor_ = ::torch::from_blob(
      data, tensor_sizes, ::at::IntArrayRef{tensor_strides}, options);
}

Tensor::~Tensor() {
  if (data_ != nullptr && pool_ != nullptr) {
    pool_->free(data_, data_size_);
  }
}

Tensor::Tensor(const ::caffe2::TypeMeta& tensor_type)
    : tensor_(
          ::at::Tensor().cpu().toType(c10::typeMetaToScalarType(tensor_type))) {
}

Tensor::Tensor(Tensor&& other) {
  *this = std::move(other);
}

Tensor& Tensor::operator=(Tensor&& other) {
  std::swap(pool_, other.pool_);
  std::swap(tensor_, other.tensor_);
  std::swap(data_, other.data_);
  std::swap(data_size_, other.data_size_);
  return *this;
};
} // namespace bytedance::bolt::torch
