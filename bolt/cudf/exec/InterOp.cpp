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

#include <arrow/c/bridge.h>
#include <arrow/io/interfaces.h>
#include <arrow/table.h>
#include <cudf/interop.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/table/table.hpp>
#include <cudf/types.hpp>
#include <cudf/utilities/error.hpp>
#include <rmm/cuda_stream_view.hpp>

#include "bolt/cudf/exec/InterOp.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/ConstantVector.h"
#include "bolt/vector/arrow/Bridge.h"

namespace bolt::cudf::exec {
::cudf::type_id fromBoltType(const bytedance::bolt::TypePtr& type) {
  switch (type->kind()) {
    case bytedance::bolt::TypeKind::BOOLEAN:
      return ::cudf::type_id::BOOL8;
    case bytedance::bolt::TypeKind::TINYINT:
      return ::cudf::type_id::INT8;
    case bytedance::bolt::TypeKind::SMALLINT:
      return ::cudf::type_id::INT16;
    case bytedance::bolt::TypeKind::INTEGER:
      if (type->isDate()) {
        return ::cudf::type_id::TIMESTAMP_DAYS;
      }
      return ::cudf::type_id::INT32;
    case bytedance::bolt::TypeKind::BIGINT:
      if (type->isShortDecimal()) {
        return ::cudf::type_id::DECIMAL64;
      }
      return ::cudf::type_id::INT64;
    case bytedance::bolt::TypeKind::REAL:
      return ::cudf::type_id::FLOAT32;
    case bytedance::bolt::TypeKind::DOUBLE:
      return ::cudf::type_id::FLOAT64;
    case bytedance::bolt::TypeKind::VARCHAR:
      return ::cudf::type_id::STRING;
    case bytedance::bolt::TypeKind::VARBINARY:
      return ::cudf::type_id::STRING;
    case bytedance::bolt::TypeKind::TIMESTAMP:
      // Timestamp type is a structure with sec and nsec.
      return ::cudf::type_id::STRUCT;
    case bytedance::bolt::TypeKind::HUGEINT:
      // Fixed-point type with __int128_t.
      if (type->isLongDecimal()) {
        return ::cudf::type_id::DECIMAL128;
      }
      break;
    case bytedance::bolt::TypeKind::ARRAY:
      return ::cudf::type_id::LIST;
    case bytedance::bolt::TypeKind::MAP: {
      auto keyType = fromBoltType(type->childAt(0));
      if (keyType == ::cudf::type_id::INT32) {
        // Dictionary type using int32 indices.
        return ::cudf::type_id::DICTIONARY32;
      }
      // TODO: It may be STRUCT.
      break;
    }
    case bytedance::bolt::TypeKind::ROW:
      return ::cudf::type_id::STRUCT;
    case bytedance::bolt::TypeKind::OPAQUE:
      // shared_ptr<void> struct
      return ::cudf::type_id::STRUCT;
    default:
      break;
  }
  BOLT_FAIL(type->toString() + " is not supported by cuDF.");
}

std::unique_ptr<::cudf::table> toCudfTable(
    const bytedance::bolt::RowVectorPtr& boltTable,
    bytedance::bolt::memory::MemoryPool* pool,
    rmm::cuda_stream_view stream,
    InterOpStats* stats) {
  // Need to flattenDictionary and flattenConstant, otherwise we observe issues
  // in the null mask.
  ArrowOptions arrowOptions{.flattenDictionary = true, .flattenConstant = true};
  ArrowArray arrowArray;
  auto boltTableBasePtr =
      std::dynamic_pointer_cast<bytedance::bolt::BaseVector>(boltTable);
  if (stats) {
    bytedance::bolt::NanosecondTimer timer(&stats->exportToArrowArrayTimeNs);
    exportToArrow(boltTableBasePtr, arrowArray, pool, arrowOptions);
  } else {
    exportToArrow(boltTableBasePtr, arrowArray, pool, arrowOptions);
  }
  ArrowSchema arrowSchema;
  if (stats) {
    bytedance::bolt::NanosecondTimer timer(&stats->exportToArrowSchemaTimeNs);
    exportToArrow(boltTableBasePtr, arrowSchema, arrowOptions);
  } else {
    exportToArrow(boltTableBasePtr, arrowSchema, arrowOptions);
  }
  auto tbl = ::cudf::from_arrow(&arrowSchema, &arrowArray, stream);

  arrowArray.release(&arrowArray);
  arrowSchema.release(&arrowSchema);

  return tbl;
}

std::unique_ptr<::cudf::column> toCudfColumn(
    const bytedance::bolt::RowVectorPtr& boltColumn,
    bytedance::bolt::memory::MemoryPool* pool,
    rmm::cuda_stream_view stream,
    InterOpStats* stats) {
  // Need to flattenDictionary and flattenConstant, otherwise we observe issues
  // in the null mask.
  ArrowOptions arrowOptions{.flattenDictionary = true, .flattenConstant = true};
  ArrowArray arrowArray;
  auto boltColumnBasePtr =
      std::dynamic_pointer_cast<bytedance::bolt::BaseVector>(boltColumn);
  if (stats) {
    bytedance::bolt::NanosecondTimer timer(&stats->exportToArrowArrayTimeNs);
    exportToArrow(boltColumnBasePtr, arrowArray, pool, arrowOptions);
  } else {
    exportToArrow(boltColumnBasePtr, arrowArray, pool, arrowOptions);
  }
  ArrowSchema arrowSchema;
  if (stats) {
    bytedance::bolt::NanosecondTimer timer(&stats->exportToArrowSchemaTimeNs);
    exportToArrow(boltColumnBasePtr, arrowSchema, arrowOptions);
  } else {
    exportToArrow(boltColumnBasePtr, arrowSchema, arrowOptions);
  }
  auto col = ::cudf::from_arrow_column(&arrowSchema, &arrowArray, stream);

  arrowArray.release(&arrowArray);
  arrowSchema.release(&arrowSchema);

  return col;
}

namespace {

std::vector<::cudf::column_metadata> getMetadata(
    std::vector<::cudf::column_view>::const_iterator begin,
    std::vector<::cudf::column_view>::const_iterator end) {
  std::vector<::cudf::column_metadata> metadata;
  size_t i = 0;
  for (auto c = begin; c < end; c++) {
    metadata.push_back(::cudf::column_metadata(std::to_string(i)));
    metadata.back().children_meta =
        getMetadata(c->child_begin(), c->child_end());
    i++;
  }
  return metadata;
}

} // namespace
    const ::cudf::table_view& cudfTable,
    bytedance::bolt::memory::MemoryPool* pool,
    rmm::cuda_stream_view stream,
    InterOpStats* stats) {
      auto metadata = getMetadata(cudfTable.begin(), cudfTable.end());

      auto arrowDeviceArray = ::cudf::to_arrow_host(cudfTable, stream);
      auto& arrowArray = arrowDeviceArray->array;

      auto arrowSchema = ::cudf::to_arrow_schema(cudfTable, metadata);
      auto boltTable = bytedance::bolt::importFromArrowAsOwner(
          *arrowSchema, arrowArray, {}, pool);
      auto castedPtr =
          std::dynamic_pointer_cast<bytedance::bolt::RowVector>(boltTable);
      BOLT_CHECK_NOT_NULL(castedPtr);
      return castedPtr;
    }

    namespace {
    template <typename T>
    std::unique_ptr<::cudf::scalar> toCudfScalarTyped(
        const bytedance::bolt::BaseVector& boltVector,
        rmm::cuda_stream_view stream,
        InterOpStats* stats) {
      using V = bytedance::bolt::ConstantVector<T>;
      auto constantVector = boltVector.as<V>();
      BOLT_CHECK_NOT_NULL(constantVector);
      if (constantVector->isNullAt(0)) {
        return ::cudf::make_numeric_scalar(
            ::cudf::data_type(fromBoltType(boltVector.type())), stream);
      }
      return std::make_unique<::cudf::numeric_scalar<T>>(
          constantVector->value(), true, stream);
    }
    } // namespace

    std::unique_ptr<::cudf::scalar> toCudfScalar(
        const bytedance::bolt::BaseVector& boltVector,
        bytedance::bolt::memory::MemoryPool* pool,
        rmm::cuda_stream_view stream,
        InterOpStats* stats) {
      switch (boltVector.typeKind()) {
        case bytedance::bolt::TypeKind::INTEGER:
          return toCudfScalarTyped<int32_t>(boltVector, stream, stats);
        case bytedance::bolt::TypeKind::BIGINT:
          return toCudfScalarTyped<int64_t>(boltVector, stream, stats);
        case bytedance::bolt::TypeKind::REAL:
          return toCudfScalarTyped<float>(boltVector, stream, stats);
        case bytedance::bolt::TypeKind::DOUBLE:
          return toCudfScalarTyped<double>(boltVector, stream, stats);
        default:
          BOLT_UNSUPPORTED(
              "Unsupported type for toCudfScalar: {}",
              boltVector.type()->toString());
      }
    }

    void recordInterOpStats(
        bytedance::bolt::exec::Operator& op,
        const InterOpStats& interOpStats) {
      auto lockedStats = op.stats().wlock();
      lockedStats->addRuntimeStat(
          "exportToArrowArrayNanos",
          bytedance::bolt::RuntimeCounter{
              static_cast<int64_t>(interOpStats.exportToArrowArrayTimeNs),
              bytedance::bolt::RuntimeCounter::Unit::kNanos});
      lockedStats->addRuntimeStat(
          "exportToArrowSchemaNanos",
          bytedance::bolt::RuntimeCounter{
              static_cast<int64_t>(interOpStats.exportToArrowSchemaTimeNs),
              bytedance::bolt::RuntimeCounter::Unit::kNanos});
    }

    } // namespace bolt::cudf::exec
