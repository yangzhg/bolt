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

#include <type/Type.h>
#include <vector/DecodedVector.h>
#include "bolt/functions/Udf.h"
namespace bytedance::bolt::functions {

/**
 * combine(map[k, v], map[k, v]) -> map[k, v]
 * combine(array[e], array[e]) -> array[e]]
 *           - Returns a combined list of two lists,
 *  or a combined map of two maps
 */
class UdfMapArrayCombine : public exec::VectorFunction {
 public:
  // todo: consider whether null behavior is correct or not.
  bool isDefaultNullBehavior() const override {
    return false;
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;

    signatures.emplace_back(exec::FunctionSignatureBuilder()
                                .knownTypeVariable("K")
                                .typeVariable("V")
                                .returnType("map(K,V)")
                                .argumentType("map(K,V)")
                                .argumentType("map(K,V)")
                                .variableArity()
                                .build());
    signatures.emplace_back(exec::FunctionSignatureBuilder()
                                .knownTypeVariable("E")
                                .returnType("array(E)")
                                .argumentType("array(E)")
                                .argumentType("array(E)")
                                .variableArity()
                                .build());
    return signatures;
  }

 private:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK_GE(args.size(), 2);
    auto argsType = args[0]->type();
    switch (argsType->kind()) {
      case TypeKind::MAP:
        return applyMap(rows, args, outputType, context, result);
      case TypeKind::ARRAY:
        return applyArray(rows, args, outputType, context, result);
      default:
        BOLT_UNSUPPORTED(
            "Unsupported argument type for combine: {}",
            mapTypeKindToName(argsType->kind()));
    }
  }

  /** Nearly copied from MapConcat.cpp apply() method.
   */
  void applyMap(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    auto mapType = args[0]->type();
    BOLT_CHECK_EQ(mapType->kind(), TypeKind::MAP);
    for (auto& arg : args) {
      BOLT_CHECK(mapType->kindEquals(arg->type()));
    }
    BOLT_CHECK(mapType->kindEquals(outputType));

    auto numArgs = args.size();
    exec::DecodedArgs decodedArgs(rows, args, context);
    vector_size_t maxSize = 0;
    for (auto i = 0; i < numArgs; i++) {
      auto decodedArg = decodedArgs.at(i);
      auto inputMap = decodedArg->base()->as<MapVector>();
      auto rawSizes = inputMap->rawSizes();
      rows.applyToSelected([&](vector_size_t row) {
        if (decodedArg->isNullAt(row)) {
          return;
        }
        maxSize += rawSizes[decodedArg->index(row)];
      });
    }

    auto keyType = outputType->asMap().keyType();
    auto valueType = outputType->asMap().valueType();

    auto* pool = context.pool();
    auto combinedKeys = BaseVector::create(keyType, maxSize, pool);
    auto combinedValues = BaseVector::create(valueType, maxSize, pool);

    // Initialize offsets and sizes to 0 so that canonicalize() will
    // work also for sparse 'rows'.
    BufferPtr offsets = allocateOffsets(rows.end(), pool);
    auto rawOffsets = offsets->asMutable<vector_size_t>();

    BufferPtr sizes = allocateSizes(rows.end(), pool);
    auto rawSizes = sizes->asMutable<vector_size_t>();

    vector_size_t offset = 0;
    rows.applyToSelected([&](vector_size_t row) {
      rawOffsets[row] = offset;
      // Reuse the last offset and size if null key must create empty map
      for (auto i = 0; i < numArgs; i++) {
        auto decodedArg = decodedArgs.at(i);
        if (decodedArg->isNullAt(row)) {
          continue; // Treat NULL maps as empty.
        }
        auto inputMap = decodedArg->base()->as<MapVector>();
        auto index = decodedArg->index(row);
        auto inputOffset = inputMap->offsetAt(index);
        auto inputSize = inputMap->sizeAt(index);
        combinedKeys->copy(
            inputMap->mapKeys().get(), offset, inputOffset, inputSize);
        combinedValues->copy(
            inputMap->mapValues().get(), offset, inputOffset, inputSize);
        offset += inputSize;
      }
      rawSizes[row] = offset - rawOffsets[row];
    });

    auto combinedMap = std::make_shared<MapVector>(
        pool,
        outputType,
        BufferPtr(nullptr),
        rows.end(),
        offsets,
        sizes,
        combinedKeys,
        combinedValues);

    MapVector::canonicalize(combinedMap, true);

    combinedKeys = combinedMap->mapKeys();
    combinedValues = combinedMap->mapValues();

    // Check for duplicate keys
    SelectivityVector uniqueKeys(offset);
    vector_size_t duplicateCnt = 0;
    rows.applyToSelected([&](vector_size_t row) {
      auto mapOffset = rawOffsets[row];
      auto mapSize = rawSizes[row];
      if (duplicateCnt) {
        rawOffsets[row] -= duplicateCnt;
      }
      for (vector_size_t i = 1; i < mapSize; i++) {
        if (combinedKeys->equalValueAt(
                combinedKeys.get(), mapOffset + i, mapOffset + i - 1)) {
          duplicateCnt++;
          // "remove" duplicate entry
          uniqueKeys.setValid(mapOffset + i - 1, false);
          rawSizes[row]--;
        }
      }
    });

    if (duplicateCnt) {
      uniqueKeys.updateBounds();
      auto uniqueCount = uniqueKeys.countSelected();

      BufferPtr uniqueIndices = allocateIndices(uniqueCount, pool);
      auto rawUniqueIndices = uniqueIndices->asMutable<vector_size_t>();
      vector_size_t index = 0;
      uniqueKeys.applyToSelected(
          [&](vector_size_t row) { rawUniqueIndices[index++] = row; });

      auto keys = BaseVector::transpose(uniqueIndices, std::move(combinedKeys));
      auto values =
          BaseVector::transpose(uniqueIndices, std::move(combinedValues));

      combinedMap = std::make_shared<MapVector>(
          pool,
          outputType,
          BufferPtr(nullptr),
          rows.end(),
          offsets,
          sizes,
          keys,
          values);
    }

    context.moveOrCopyResult(combinedMap, rows, result);
  }

  void applyArray(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    auto arrayType = args[0]->type();
    BOLT_CHECK_EQ(arrayType->kind(), TypeKind::ARRAY);
    for (auto& arg : args) {
      BOLT_CHECK(arrayType->kindEquals(arg->type()));
    }
    BOLT_CHECK(arrayType->kindEquals(outputType));

    auto numArgs = args.size();
    exec::DecodedArgs decodedArgs(rows, args, context);
    vector_size_t maxSize = 0;
    for (auto i = 0; i < numArgs; i++) {
      auto decodedArg = decodedArgs.at(i);
      auto inputArray = decodedArg->base()->as<ArrayVector>();
      auto rawSizes = inputArray->rawSizes();
      rows.applyToSelected([&](vector_size_t row) {
        if (decodedArg->isNullAt(row)) {
          return;
        }
        maxSize += rawSizes[decodedArg->index(row)];
      });
    }

    auto elementType = outputType->asArray().elementType();

    auto* pool = context.pool();
    auto combinedElements = BaseVector::create(elementType, maxSize, pool);

    // Initialize offsets and sizes to 0 so that canonicalize() will
    // work also for sparse 'rows'.
    BufferPtr offsets = allocateOffsets(rows.end(), pool);
    auto rawOffsets = offsets->asMutable<vector_size_t>();

    BufferPtr sizes = allocateSizes(rows.end(), pool);
    auto rawSizes = sizes->asMutable<vector_size_t>();

    vector_size_t offset = 0;
    rows.applyToSelected([&](vector_size_t row) {
      rawOffsets[row] = offset;
      // Reuse the last offset and size if null key must create empty map
      for (auto i = 0; i < numArgs; i++) {
        auto decodedArg = decodedArgs.at(i);
        if (decodedArg->isNullAt(row)) {
          continue; //
        }
        auto inputArray = decodedArg->base()->as<ArrayVector>();
        auto index = decodedArg->index(row);
        auto inputOffset = inputArray->offsetAt(index);
        auto inputSize = inputArray->sizeAt(index);
        combinedElements->copy(
            inputArray->elements().get(), offset, inputOffset, inputSize);
        offset += inputSize;
      }
      rawSizes[row] = offset - rawOffsets[row];
    });

    auto combinedArray = std::make_shared<ArrayVector>(
        pool,
        outputType,
        BufferPtr(nullptr),
        rows.end(),
        offsets,
        sizes,
        combinedElements);

    context.moveOrCopyResult(combinedArray, rows, result);
  }
};

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_map_or_array_combine,
    UdfMapArrayCombine::signatures(),
    std::make_unique<UdfMapArrayCombine>());

} // namespace bytedance::bolt::functions
