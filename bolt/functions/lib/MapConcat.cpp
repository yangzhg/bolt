/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "MapConcat.h"
#include "bolt/expression/Expr.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/vector/TypeAliases.h"
namespace bytedance::bolt::functions {
namespace {

// See documentation at https://prestodb.io/docs/current/functions/map.html
template <bool EmptyForNull, bool AllowSingleArg>
class MapConcatFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return !EmptyForNull;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // Ensure all input types and output type are of the same map type.
    const TypePtr& mapType = args[0]->type();
    BOLT_CHECK_EQ(mapType->kind(), TypeKind::MAP);
    for (auto& arg : args) {
      BOLT_CHECK(mapType->kindEquals(arg->type()));
    }
    BOLT_CHECK(mapType->kindEquals(outputType));

    const size_t numArgs = args.size();
    if constexpr (!AllowSingleArg) {
      BOLT_CHECK_GE(numArgs, 2);
    }

    exec::DecodedArgs decodedArgs(rows, args, context);
    vector_size_t maxSize = 0;
    for (auto i = 0; i < numArgs; i++) {
      auto decodedArg = decodedArgs.at(i);
      auto inputMap = decodedArg->base()->as<MapVector>();
      auto rawSizes = inputMap->rawSizes();
      rows.applyToSelected([&](vector_size_t row) {
        if (EmptyForNull && decodedArg->isNullAt(row)) {
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
        if (EmptyForNull && decodedArg->isNullAt(row)) {
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
    bool throwExceptionOnDuplicateMapKeys = false;
    if (auto* ctx = context.execCtx()->queryCtx()) {
      throwExceptionOnDuplicateMapKeys =
          ctx->queryConfig().throwExceptionOnDuplicateMapKeys();
    }
    rows.applyToSelected([&](vector_size_t row) {
      auto mapOffset = rawOffsets[row];
      auto mapSize = rawSizes[row];
      if (duplicateCnt) {
        rawOffsets[row] -= duplicateCnt;
      }
      for (vector_size_t i = 1; i < mapSize; i++) {
        if (combinedKeys->equalValueAt(
                combinedKeys.get(), mapOffset + i, mapOffset + i - 1)) {
          if (throwExceptionOnDuplicateMapKeys) {
            const auto duplicateKey = combinedKeys->wrappedVector()->toString(
                combinedKeys->wrappedIndex(mapOffset + i));
            BOLT_USER_FAIL("Duplicate map key {} was found.", duplicateKey);
          }
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

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // map(K,V), map(K,V), ... -> map(K,V)
    return {exec::FunctionSignatureBuilder()
                .knownTypeVariable("K")
                .typeVariable("V")
                .returnType("map(K,V)")
                .argumentType("map(K,V)")
                .argumentType("map(K,V)")
                .variableArity()
                .build()};
  }
};
} // namespace

void registerMapConcatFunction(const std::string& name) {
  exec::registerVectorFunction(
      name,
      MapConcatFunction</*EmptyForNull=*/false, /*AllowSingleArg=*/false>::
          signatures(),
      std::make_unique<MapConcatFunction<
          /*EmptyForNull=*/false,
          /*AllowSingleArg=*/false>>());
}

void registerMapConcatAllowSingleArg(const std::string& name) {
  exec::registerVectorFunction(
      name,
      MapConcatFunction</*EmptyForNull=*/false, /*AllowSingleArg=*/true>::
          signatures(),
      std::make_unique<MapConcatFunction<
          /*EmptyForNull=*/false,
          /*AllowSingleArg=*/true>>());
}

void registerMapConcatEmptyNullsFunction(const std::string& name) {
  exec::registerVectorFunction(
      name,
      MapConcatFunction</*EmptyForNull=*/true, /*AllowSingleArg=*/false>::
          signatures(),
      std::make_unique<MapConcatFunction<
          /*EmptyForNull=*/true,
          /*AllowSingleArg=*/false>>());
}
} // namespace bytedance::bolt::functions
