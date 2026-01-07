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

#include "bolt/functions/sparksql/JsonTuple.h"
#include "bolt/expression/VectorWriters.h"
#include "bolt/functions/prestosql/json/JsonExtractor.h"
namespace bytedance::bolt::functions::sparksql {
namespace {
template <bool legacy>
class JsonTupleFunction : public exec::VectorFunction {
 private:
  const bool useSonicLibrary_ = true;

 public:
  JsonTupleFunction(const bool useSonicLibrary)
      : useSonicLibrary_(useSonicLibrary) {}
  bool isDefaultNullBehavior() const override {
    return false;
  }
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BaseVector::ensureWritable(rows, ARRAY(VARCHAR()), context.pool(), result);
    exec::LocalDecodedVector input(context, *args[0], rows);
    exec::VectorWriter<Array<Varchar>> resultWriter;
    resultWriter.init(*result->as<ArrayVector>());
    BOLT_CHECK_EQ(args[1]->type()->kind(), TypeKind::ARRAY);
    BOLT_CHECK_EQ(
        args[1]->type()->asArray().elementType()->kind(), TypeKind::VARCHAR);
    ArrayVector* arrayVector{nullptr};
    if (args[1]->encoding() == VectorEncoding::Simple::ARRAY) {
      arrayVector = args[1]->as<ArrayVector>();
    } else if (args[1]->isConstantEncoding()) {
      arrayVector = args[1]
                        ->as<ConstantVector<bolt::ComplexType>>()
                        ->valueVector()
                        ->as<ArrayVector>();
    }
    auto offsets = arrayVector->offsets()->as<vector_size_t>();
    auto lengths = arrayVector->sizes()->as<vector_size_t>();

    vector_size_t offset = offsets[0];
    vector_size_t length = lengths[0];
    auto elementsVector = arrayVector->elements()->as<FlatVector<StringView>>();
    BOLT_CHECK(elementsVector != nullptr);
    BOLT_CHECK_GE(length, 0);
    BOLT_CHECK_GE(offset, 0);
    std::vector<folly::Optional<folly::StringPiece>> paths{
        static_cast<size_t>(length), folly::none};
    for (vector_size_t i = 0; i < length; ++i) {
      if (!elementsVector->isNullAt(offset + i)) {
        StringView sv = elementsVector->valueAt(offset + i);
        // if StringView is inlined, the sv.data() is the address of the sv + 4,
        // sv is a stack variable,
        // it will be freed after the sv is destroyed, so we should use the
        // original address, the literal value 4 is the size_ of StringView
        if (sv.isInline()) {
          paths[i] = folly::StringPiece{
              (const char*)(elementsVector->rawValues() + offset + i) + 4,
              sv.size()};
        } else {
          paths[i] = folly::StringPiece{sv.data(), sv.size()};
        }
      }
    }
    rows.applyToSelected([&](auto row) {
      resultWriter.setOffset(row);
      auto& arrayWriter = resultWriter.current();
      if (input->isNullAt(row)) {
        for (int i = 0; i < length; ++i) {
          arrayWriter.add_null();
        }
        resultWriter.commit();
        return;
      }
      auto jsonStr = input->valueAt<StringView>(row);
      auto vals = jsonExtractTuple(jsonStr, paths, legacy, useSonicLibrary_);
      for (size_t i = 0; i < vals.size(); ++i) {
        auto& val = vals[i];
        val.hasValue() ? arrayWriter.add_item() = StringView(val.value())
                       : arrayWriter.add_null();
      }
      resultWriter.commit();
    });
    resultWriter.finish();
  }
};
} // namespace
std::shared_ptr<exec::VectorFunction> makeJsonTuple(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  const bool useSonicLibrary = config.useSonicJson();
  if (inputArgs.size() != 3) {
    BOLT_FAIL("JsonTupleFunction: expected 3 arguments");
  }
  BOLT_CHECK_EQ(inputArgs[0].type->kind(), TypeKind::VARCHAR);
  BOLT_CHECK_EQ(inputArgs[1].type->kind(), TypeKind::ARRAY);
  BOLT_CHECK_EQ(inputArgs[1].type->childAt(0)->kind(), TypeKind::VARCHAR);
  BOLT_CHECK_EQ(inputArgs[2].type->kind(), TypeKind::BOOLEAN);
  bool legacy = true;
  BaseVector* boolVector = inputArgs[2].constantValue.get();
  if (!boolVector || !boolVector->isConstantEncoding()) {
    BOLT_USER_FAIL("{} requires a constant bool as the second argument.", name);
  }
  legacy = boolVector->as<ConstantVector<bool>>()->valueAt(0);
  if (!boolVector || !boolVector->isConstantEncoding()) {
    BOLT_USER_FAIL("{} requires a constant bool as the second argument.", name);
  }
  if (legacy) {
    return std::make_shared<JsonTupleFunction<true>>(useSonicLibrary);
  } else {
    return std::make_shared<JsonTupleFunction<false>>(useSonicLibrary);
  }
}
std::vector<std::shared_ptr<exec::FunctionSignature>> jsonTupleSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("array(varchar)")
              .argumentType("varchar")
              .argumentType("array(varchar)")
              .argumentType("boolean")
              .build()};
}
} // namespace bytedance::bolt::functions::sparksql