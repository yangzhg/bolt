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

#include <cudf/table/table.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/default_stream.hpp>
#include <rmm/cuda_stream_view.hpp>

#include "bolt/exec/Operator.h"
#include "bolt/vector/ComplexVector.h"

namespace bolt::cudf::exec {

::cudf::type_id fromBoltType(const bytedance::bolt::TypePtr& type);

struct InterOpStats {
  uint64_t exportToArrowArrayTimeNs{0};
  uint64_t exportToArrowSchemaTimeNs{0};
};

std::unique_ptr<::cudf::table> toCudfTable(
    const bytedance::bolt::RowVectorPtr& boltTable,
    bytedance::bolt::memory::MemoryPool* pool,
    rmm::cuda_stream_view stream = ::cudf::get_default_stream(),
    InterOpStats* stats = nullptr);

std::unique_ptr<::cudf::column> toCudfColumn(
    const bytedance::bolt::RowVectorPtr& boltColumn,
    bytedance::bolt::memory::MemoryPool* pool,
    rmm::cuda_stream_view stream = ::cudf::get_default_stream(),
    InterOpStats* stats = nullptr);

bytedance::bolt::RowVectorPtr toBoltRowVector(
    const ::cudf::table_view& cudfTable,
    bytedance::bolt::memory::MemoryPool* pool,
    rmm::cuda_stream_view stream = ::cudf::get_default_stream(),
    InterOpStats* stats = nullptr);

std::unique_ptr<::cudf::scalar> toCudfScalar(
    const bytedance::bolt::BaseVector& boltVector,
    bytedance::bolt::memory::MemoryPool* pool,
    rmm::cuda_stream_view stream = ::cudf::get_default_stream(),
    InterOpStats* stats = nullptr);

void recordInterOpStats(
    bytedance::bolt::exec::Operator& op,
    const InterOpStats& interOpStats);

} // namespace bolt::cudf::exec
