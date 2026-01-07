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

#include <arrow/c/bridge.h>
#include <arrow/type.h>
#include "bolt/expression/Expr.h"
#include "bolt/expression/TypeSignature.h"
#include "bolt/functions/colocate/client/UDFClientManager.h"
#include "bolt/type/Type.h"
#include "bolt/vector/arrow/Bridge.h"

using arrow::RecordBatch;
using arrow::Schema;
using bytedance::bolt::exec::TypeSignature;
namespace bytedance::bolt::functions {

struct ColocateFunctionMetadata : exec::VectorFunctionMetadata {
  std::shared_ptr<UDFClientManager> manager;
};

std::shared_ptr<RecordBatch> boltToArrowVector(
    std::vector<VectorPtr>& args,
    memory::MemoryPool* pool);

void arrowVectorsToBolt(
    std::vector<std::shared_ptr<RecordBatch>> arrowBatch,
    VectorPtr* result,
    memory::MemoryPool* pool);

std::vector<std::shared_ptr<RecordBatch>> retryCallServer(
    const std::shared_ptr<UDFClientManager> clientManager,
    const std::vector<std::string>& path,
    std::shared_ptr<RecordBatch>& arrowVector,
    const unsigned int max_retries,
    const TimeoutDuration& timeout);

std::shared_ptr<arrow::Schema> boltTypesToArrowSchema(
    const std::vector<TypeSignature>& types,
    const std::string& prefix = "");

std::string SerializeSchema(const std::shared_ptr<arrow::Schema>& schema);

std::string boltTypeToArrowTypeString(const TypePtr& type);

std::string to_signature(
    const std::string& function,
    const std::vector<TypePtr>& args) noexcept;

VectorPtr decodeSelectedVector(
    const SelectivityVector& rows,
    const VectorPtr& vector);
} // namespace bytedance::bolt::functions
