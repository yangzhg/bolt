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

#include "bolt/functions/colocate/client/ColocateUtilities.h"
#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/base64.h>
#include <bolt/type/Type.h>
#include <sstream>
#include "bolt/functions/colocate/client/Exception.h"
namespace bytedance::bolt::functions {
std::shared_ptr<RecordBatch> boltToArrowVector(
    std::vector<VectorPtr>& args,
    memory::MemoryPool* pool) {
  std::vector<std::shared_ptr<arrow::Field>> fields;

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  ArrowOptions options{
      .flattenDictionary = true,
      .flattenConstant = true,
      .exportToView = false,
      .useLargeString = true,
  };
  auto rows = 0;
  for (auto i = 0; i < args.size(); i++) {
    const auto& col = args[i];
    ArrowSchema arrow_schema{};
    ArrowArray array{};
    exportToArrow(col, arrow_schema, options);
    exportToArrow(col, array, pool, options);
    auto importTypeResult = arrow::ImportType(&arrow_schema);
    if (!importTypeResult.ok()) {
      BOLT_FAIL(
          "Unable import arrow schema : " +
          importTypeResult.status().ToString());
    }
    const auto& arrow_datatype = importTypeResult.ValueUnsafe();
    auto importArrayResult = ImportArray(&array, arrow_datatype);
    if (!importArrayResult.ok()) {
      BOLT_FAIL(
          "Unable import arrow array : " +
          importArrayResult.status().ToString());
    }
    const auto& arrow_array = importArrayResult.ValueUnsafe();
    std::stringstream ss;
    ss << "input" << i + 1;
    auto field = arrow::field(ss.str(), arrow_datatype);
    fields.push_back(field);
    arrays.push_back(arrow_array);
    rows = col->size();
  }
  auto schema = arrow::schema(fields);
  return RecordBatch::Make(schema, (int64_t)rows, arrays);
}

void arrowVectorsToBolt(
    std::vector<std::shared_ptr<RecordBatch>> arrowBatch,
    VectorPtr* result,
    memory::MemoryPool* pool) {
  assert(arrowBatch.size() == 1);
  const auto& arrowResult = arrowBatch.at(0);
  auto columns = arrowResult->columns();
  assert(columns.size() == 1);
  const auto& returned_array = columns.at(0);
  ArrowArray returned_arrow_array{};
  ArrowOptions options;
  ArrowSchema returned_arrow_schema{};
  auto status = ExportArray(
      *returned_array, &returned_arrow_array, &returned_arrow_schema);
  *result = importFromArrowAsOwner(
      returned_arrow_schema, returned_arrow_array, options, pool);
}

std::vector<std::shared_ptr<RecordBatch>> retryCallServer(
    const std::shared_ptr<UDFClientManager> clientManager,
    const std::vector<std::string>& path,
    std::shared_ptr<RecordBatch>& arrowVector,
    const unsigned int max_retries,
    const TimeoutDuration& timeout) {
  std::vector<std::shared_ptr<RecordBatch>> result;
  auto start = high_resolution_clock::now();
  VLOG(1) << "Acquired client. ";
  for (unsigned int attempt = 1; attempt <= max_retries; ++attempt) {
    try {
      auto client = clientManager->AcquireRandom();
      VLOG(1) << "Acquired a colocate UDF client.";
      std::shared_ptr<void> guard(
          nullptr, [&](void*) { clientManager->Release(client); });
      result = client->Call(path, arrowVector, timeout);
      break;
    } catch (const ColocateTimeoutException& e) {
      if (attempt == max_retries) {
        std::string msg = "Retries exceeded " + std::to_string(max_retries) +
            " times.[" + e.ToString() + "]";
        LOG(ERROR) << msg;
        BOLT_FAIL(msg);
      } else {
        LOG(WARNING) << e.ToString();
      }
    }
  }
  auto end = high_resolution_clock::now();
  auto functionName = path[1];
  auto duration = std::chrono::duration_cast<milliseconds>(end - start);
  VLOG(1) << "Called colocate UDF " << functionName << " with "
          << result[0]->num_rows() << " rows costs " << duration.count()
          << " ms.";
  return result;
}

std::shared_ptr<arrow::DataType> ConvertToArrowType(
    const TypeSignature& typeSignature,
    const std::string& prefix) {
  const std::string& baseType = typeSignature.baseName();
  if (baseType == "boolean") {
    return arrow::boolean();
  }
  if (baseType == "tinyint") {
    return arrow::int8();
  }
  if (baseType == "smallint") {
    return arrow::int16();
  }
  if (baseType == "integer") {
    return arrow::int32();
  }
  if (baseType == "bigint") {
    return arrow::int64();
  }
  if (baseType == "real") {
    return arrow::float32();
  }
  if (baseType == "double") {
    return arrow::float64();
  }
  if (baseType == "varchar") {
    return arrow::utf8();
  }
  if (baseType == "varbinary") {
    return arrow::binary();
  }
  if (baseType == "date") {
    return arrow::date32();
  }
  if (baseType == "decimal") {
    // Extract precision and scale from typeSignature
    const auto& precisionSignature = typeSignature.parameters()[0];
    const auto& scaleSignature = typeSignature.parameters()[1];
    int32_t precision = std::stoi(precisionSignature.baseName());
    int32_t scale = std::stoi(scaleSignature.baseName());

    if (precision <= 18) {
      return arrow::decimal(precision, scale);
    }
    return arrow::decimal(precision, scale);
  }
  if (baseType == "timestamp") {
    return arrow::timestamp(arrow::TimeUnit::NANO);
  }
  if (baseType == "array") {
    auto elementType = typeSignature.parameters()[0];
    auto elementArrowType = ConvertToArrowType(elementType, prefix);
    return arrow::list(elementArrowType);
  }
  if (baseType == "map") {
    auto keyType = ConvertToArrowType(typeSignature.parameters()[0], prefix);
    auto valueType = ConvertToArrowType(typeSignature.parameters()[1], prefix);
    return arrow::map(keyType, valueType);
  }
  if (baseType == "row") {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    auto types = typeSignature.parameters();
    auto name = typeSignature.rowFieldName().value_or("input");
    for (size_t i = 0; i < types.size(); ++i) {
      fields.push_back(arrow::field(
          name + std::to_string(i + 1), ConvertToArrowType(types[i], prefix)));
    }
    return arrow::struct_(fields);
  }

  BOLT_FAIL("Unsupported Bolt type conversion to Arrow");
}

std::shared_ptr<arrow::Schema> boltTypesToArrowSchema(
    const std::vector<TypeSignature>& types,
    const std::string& prefix) {
  std::vector<std::shared_ptr<arrow::Field>> arrowFields;

  for (int i = 0; i < types.size(); i++) {
    auto boltTypeSignature = types[i];
    auto arrowType = ConvertToArrowType(boltTypeSignature, prefix);
    arrowFields.push_back(
        arrow::field(prefix + std::to_string(i + 1), arrowType));
  }
  return arrow::schema(arrowFields);
}

std::string SerializeSchema(const std::shared_ptr<arrow::Schema>& schema) {
  // Serialize the schema to a buffer
  auto result =
      arrow::ipc::SerializeSchema(*schema, arrow::default_memory_pool());
  if (!result.ok()) {
    BOLT_FAIL("Failed to serialize schema: " + result.status().ToString());
  }
  std::shared_ptr<arrow::Buffer> buffer = result.ValueUnsafe();
  // Schema can have special char, use base64 encoding to pass the schema.
  std::string binary_data(
      reinterpret_cast<const char*>(buffer->data()), buffer->size());
  return arrow::util::base64_encode(binary_data);
}

std::string boltTypeToArrowTypeString(const TypePtr& type) {
  switch (type->kind()) {
    case bytedance::bolt::TypeKind::BOOLEAN:
      return "bool";
    case bytedance::bolt::TypeKind::TINYINT:
      return "int8";
    case bytedance::bolt::TypeKind::SMALLINT:
      return "int16";
    case bytedance::bolt::TypeKind::INTEGER:
      return "int32";
    case bytedance::bolt::TypeKind::BIGINT:
      return "int64";
    case bytedance::bolt::TypeKind::HUGEINT:
      return "int128";
    case bytedance::bolt::TypeKind::REAL:
      return "float";
    case bytedance::bolt::TypeKind::DOUBLE:
      return "double";
    case bytedance::bolt::TypeKind::VARCHAR:
      return "utf8";
    case bytedance::bolt::TypeKind::VARBINARY:
      return "binary";
    case bytedance::bolt::TypeKind::TIMESTAMP:
      return "timestamp";
    case bytedance::bolt::TypeKind::ARRAY: {
      const auto& arrayType = type->asArray();
      std::string elementType =
          boltTypeToArrowTypeString(arrayType.elementType());
      return "array(" + elementType + ")";
    }
    case bytedance::bolt::TypeKind::MAP: {
      const auto& mapType = type->asMap();
      std::string keyType = boltTypeToArrowTypeString(mapType.keyType());
      std::string valueType = boltTypeToArrowTypeString(mapType.valueType());
      return "map(" + keyType + ", " + valueType + ")";
    }
    case bytedance::bolt::TypeKind::ROW: {
      // For Struct (ROW in Bolt), recursively get field types
      const auto& rowType = type->asRow();
      std::stringstream ss;
      ss << "struct(";
      for (size_t i = 0; i < rowType.size(); ++i) {
        if (i > 0)
          ss << ", ";
        ss << boltTypeToArrowTypeString(rowType.childAt(i));
      }
      ss << ")";
      return ss.str();
    }
    default:
      BOLT_FAIL("Unsupported type " + type->toString() + " for conversion");
  }
}

std::string to_signature(
    const std::string& function,
    const std::vector<TypePtr>& args) noexcept {
  std::stringstream ss;
  ss << function << "(";
  for (size_t i = 0; i < args.size(); ++i) {
    if (i > 0)
      ss << ", ";
    ss << boltTypeToArrowTypeString(args[i]);
  }
  ss << ")";
  return ss.str();
}

// TODO: add rebuild vector SelectivityVector if see performance downgrade
VectorPtr decodeSelectedVector(
    const SelectivityVector& rows,
    const VectorPtr& vector) {
  DecodedVector decoded;
  decoded.decode(*vector, rows);
  BOLT_CHECK(vector, "vector is null!");
  return vector;
}
}; // namespace bytedance::bolt::functions
