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

#include <cuda.h>
#include <cudf/utilities/memory_resource.hpp>

#include "bolt/common/base/Exceptions.h"
#include "bolt/cudf/tests/CudfResource.h"

namespace bolt::cudf::test {

// static
CudfResource& CudfResource::getInstance() {
  static CudfResource instance;
  return instance;
}

CudfResource::CudfResource() : isInitialized_(false) {}

void CudfResource::initialize() {
  BOLT_CHECK(
      !isInitialized_,
      "CudfResource::initialize() is expected to be called only once.");

  cudaFree(nullptr); // Initialize CUDA context at startup

  // BUG in RMM(v25.06): To use "async" memory pool, build libcudf with a
  // CMake variable CUDF_BUILD_STREAMS_TEST_UTIL=OFF. If you ran into a
  // problem of invalid stream, you might want to use different type of
  // memory resource such as "pool".
  mr_ = std::make_shared<rmm::mr::cuda_async_memory_resource>();
  ::cudf::set_current_device_resource(mr_.get());

  isInitialized_ = true;
}

void CudfResource::finalize() {
  BOLT_CHECK(isInitialized_);
  mr_.reset();
  isInitialized_ = false;
}

} // namespace bolt::cudf::test