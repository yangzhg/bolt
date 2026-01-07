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

#include "bolt/expression/VectorFunction.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/NullsBuilder.h"
namespace bytedance::bolt::functions {
namespace {

class ArrayElementFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK_EQ(args.size(), 1);
    BOLT_CHECK(args[0]->type()->isArray());
    auto arrayArg = args[0];
    VectorPtr localResult;
    auto* pool = context.pool();
    BufferPtr indices = allocateIndices(rows.end(), pool);
    auto rawIndices = indices->asMutable<vector_size_t>();
    // Create nulls for lazy initialization.
    NullsBuilder nullsBuilder(rows.end(), pool);

    exec::LocalDecodedVector arrayHolder(context, *arrayArg, rows);
    auto decodedArray = arrayHolder.get();
    auto baseArray = decodedArray->base()->as<ArrayVector>();
    auto arrayIndices = decodedArray->indices();

    auto baseRawOffsets = baseArray->rawOffsets();

    rows.applyToSelected([&](auto row) {
      if (baseArray->isNullAt(row) || baseArray->sizeAt(row) == 0) {
        nullsBuilder.setNull(row);
        return;
      }
      if (baseArray->sizeAt(row) > 1) {
        BOLT_USER_FAIL(
            "Array element function only supports arrays of size 1, but got {}",
            baseArray->sizeAt(row));
      }
      const auto elementIndex = baseRawOffsets[arrayIndices[row]];
      rawIndices[row] = elementIndex;
    });

    // Subscript into empty arrays always returns NULLs. Check added at the end
    // to ensure user error checks for indices are not skipped.
    if (baseArray->elements()->size() == 0) {
      localResult = BaseVector::createNullConstant(
          baseArray->elements()->type(), rows.end(), context.pool());
    } else {
      localResult = BaseVector::wrapInDictionary(
          nullsBuilder.build(), indices, rows.end(), baseArray->elements());
    }
    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // array(T)-> T
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("T")
                .argumentType("array(T)")
                .build()};
  }
};
} // namespace

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_array_element,
    ArrayElementFunction::signatures(),
    std::make_unique<ArrayElementFunction>());
} // namespace bytedance::bolt::functions