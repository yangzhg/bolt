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

#include <memory>

#include <ATen/core/Tensor.h>

#include "bolt/common/memory/MemoryPool.h"

namespace bytedance::bolt::torch {
/// libtorch tensor wrapper with RAII memory management.
///
/// Normally, `torch::from_blob()` is the go to method to create a tensor
/// from a custom memory area. The `deleter` argument could be used to
/// deallocate the custom memory area backing the tensor data when it is
/// destroy. However, the `deleter` function is a global static function that
/// can't store RAII objects like the pool used to allocate the `data`
/// pointer. It is possible to store the pool pointer value as well as its
/// size as part of the allocation. However, this is unsafe to use because the
/// tensor may outlive the memory pool. Moreover, managing pointer alignment to
/// preserve performance becomes is unnecessarily tricky.
class Tensor {
 public:
  Tensor(const Tensor&) = delete;
  Tensor& operator=(const Tensor&) = delete;
  Tensor(Tensor&& other);
  Tensor& operator=(Tensor&& other);

  // empty tensor.
  Tensor() = default;

  // empty tensor.
  Tensor(const ::caffe2::TypeMeta& tensor_type);

  Tensor(
      void* data,
      size_t data_size,
      ::caffe2::TypeMeta tensor_type,
      ::at::IntArrayRef tensor_sizes,
      ::bytedance::bolt::memory::MemoryPool* pool);

  ~Tensor();

  const ::at::Tensor& get() const {
    return tensor_;
  }

  ::at::Tensor& get_mut() {
    return tensor_;
  }

 private:
  ::bytedance::bolt::memory::MemoryPool* pool_;
  ::at::Tensor tensor_{};
  void* data_{nullptr};
  size_t data_size_{0};
};
} // namespace bytedance::bolt::torch
