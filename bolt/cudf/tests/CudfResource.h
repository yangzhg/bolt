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

#if defined BOLT_HAS_CUDF && BOLT_HAS_CUDF == 1

#include <rmm/mr/device/cuda_async_memory_resource.hpp>

namespace bolt::cudf::test {
class CudfResource {
 public:
  static CudfResource& getInstance();

  // It should be called only once in the whole process.
  // It is not thread safe.
  // It should be called before any cudf operation.
  void initialize();

  // Need to call finalize to release cuda memory resource correctly.
  // Because it is static object, its destructor is called too late.
  void finalize();

 private:
  CudfResource();
  CudfResource(const CudfResource&) = delete;
  CudfResource& operator=(const CudfResource&) = delete;

  bool isInitialized_;
  std::shared_ptr<rmm::mr::device_memory_resource> mr_;
};
} // namespace bolt::cudf::test

#endif