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

#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/prestosql/json/SIMDJsonUtil.h"
#include "bolt/functions/prestosql/types/JsonType.h"
#include "bolt/vector/ConstantVector.h"
#include "bolt/vector/FlatVector.h"

#include <sonic/sonic.h>
#include "sonic/dom/parser.h"
namespace bytedance::bolt::functions {

namespace {
class JsonFormatFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VectorPtr localResult;

    // Input can be constant or flat.
    assert(args.size() > 0);
    const auto& arg = args[0];
    if (arg->isConstantEncoding()) {
      auto value = arg->as<ConstantVector<StringView>>()->valueAt(0);
      localResult = std::make_shared<ConstantVector<StringView>>(
          context.pool(), rows.end(), false, VARCHAR(), std::move(value));
    } else {
      auto flatInput = arg->asFlatVector<StringView>();

      auto stringBuffers = flatInput->stringBuffers();
      BOLT_CHECK_LE(rows.end(), flatInput->size());
      localResult = std::make_shared<FlatVector<StringView>>(
          context.pool(),
          VARCHAR(),
          nullptr,
          rows.end(),
          flatInput->values(),
          std::move(stringBuffers));
    }

    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // json -> varchar
    return {exec::FunctionSignatureBuilder()
                .returnType("varchar")
                .argumentType("json")
                .build()};
  }
};

class JsonParseFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& type,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    folly::call_once(initUseSonic_, [&] {
      useSonic_ =
          context.execCtx()->queryCtx()->queryConfig().enableSonicJsonParse();
    });

    if (useSonic_) {
      applySonicCpp(rows, args, type, context, result);
    } else {
      applySimdJson(rows, args, type, context, result);
    }
  }

  void applySimdJson(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    // Initialize errors here so that we get the proper exception context.
    folly::call_once(
        initializeErrors_, [this] { simdjsonErrorsToExceptions(errors_); });

    VectorPtr localResult;

    // Input can be constant or flat.
    assert(args.size() > 0);
    const auto& arg = args[0];
    if (arg->isConstantEncoding()) {
      auto value = arg->as<ConstantVector<StringView>>()->valueAt(0);
      paddedInput_.resize(value.size() + simdjson::SIMDJSON_PADDING);
      memcpy(paddedInput_.data(), value.data(), value.size());
      if (auto error = parse(value.size())) {
        context.setErrors(rows, errors_[error]);
        return;
      }
      localResult = std::make_shared<ConstantVector<StringView>>(
          context.pool(), rows.end(), false, JSON(), std::move(value));
    } else {
      auto flatInput = arg->asFlatVector<StringView>();

      auto stringBuffers = flatInput->stringBuffers();
      BOLT_CHECK_LE(rows.end(), flatInput->size());

      size_t maxSize = 0;
      rows.applyToSelected([&](auto row) {
        auto value = flatInput->valueAt(row);
        maxSize = std::max(maxSize, value.size());
      });
      paddedInput_.resize(maxSize + simdjson::SIMDJSON_PADDING);
      rows.applyToSelected([&](auto row) {
        auto value = flatInput->valueAt(row);
        memcpy(paddedInput_.data(), value.data(), value.size());
        if (auto error = parse(value.size())) {
          context.setBoltExceptionError(row, errors_[error]);
        }
      });
      localResult = std::make_shared<FlatVector<StringView>>(
          context.pool(),
          JSON(),
          nullptr,
          rows.end(),
          flatInput->values(),
          std::move(stringBuffers));
    }

    context.moveOrCopyResult(localResult, rows, result);
  }

  void applySonicCpp(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    VectorPtr localResult;

    // Input can be constant or flat.
    assert(args.size() > 0);
    const auto& arg = args[0];
    sonic_json::Document doc;
    if (arg->isConstantEncoding()) {
      auto value = arg->as<ConstantVector<StringView>>()->valueAt(0);
      doc.Parse(static_cast<std::string_view>(value));
      if (doc.HasParseError()) {
        context.setErrors(
            rows,
            std::make_exception_ptr(std::invalid_argument(
                sonic_json::ErrorMsg(doc.GetParseError()))));
        return;
      }
      localResult = std::make_shared<ConstantVector<StringView>>(
          context.pool(), rows.end(), false, JSON(), std::move(value));
    } else {
      auto flatInput = arg->asFlatVector<StringView>();

      auto stringBuffers = flatInput->stringBuffers();
      BOLT_CHECK_LE(rows.end(), flatInput->size());

      size_t maxSize = 0;
      rows.applyToSelected([&](auto row) {
        auto value = flatInput->valueAt(row);
        maxSize = std::max(maxSize, value.size());
      });
      rows.applyToSelected([&](auto row) {
        auto value = flatInput->valueAt(row);
        doc.Parse(static_cast<std::string_view>(value));
        if (doc.HasParseError()) {
          context.setBoltExceptionError(
              row,
              std::make_exception_ptr(std::invalid_argument(
                  sonic_json::ErrorMsg(doc.GetParseError()))));
        }
      });
      localResult = std::make_shared<FlatVector<StringView>>(
          context.pool(),
          JSON(),
          nullptr,
          rows.end(),
          flatInput->values(),
          std::move(stringBuffers));
    }

    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // varchar -> json
    return {exec::FunctionSignatureBuilder()
                .returnType("json")
                .argumentType("varchar")
                .build()};
  }

 private:
  simdjson::error_code parse(size_t size) const {
    simdjson::padded_string_view paddedInput(
        paddedInput_.data(), size, paddedInput_.size());
    SIMDJSON_ASSIGN_OR_RAISE(auto doc, simdjsonParse(paddedInput));
    SIMDJSON_TRY(validate<simdjson::ondemand::document&>(doc));
    if (!doc.at_end()) {
      return simdjson::TRAILING_CONTENT;
    }
    return simdjson::SUCCESS;
  }

  template <typename T>
  static simdjson::error_code validate(T value) {
    SIMDJSON_ASSIGN_OR_RAISE(auto type, value.type());
    switch (type) {
      case simdjson::ondemand::json_type::array: {
        SIMDJSON_ASSIGN_OR_RAISE(auto array, value.get_array());
        for (auto elementOrError : array) {
          SIMDJSON_ASSIGN_OR_RAISE(auto element, elementOrError);
          SIMDJSON_TRY(validate(element));
        }
        return simdjson::SUCCESS;
      }
      case simdjson::ondemand::json_type::object: {
        SIMDJSON_ASSIGN_OR_RAISE(auto object, value.get_object());
        for (auto fieldOrError : object) {
          SIMDJSON_ASSIGN_OR_RAISE(auto field, fieldOrError);
          SIMDJSON_TRY(validate(field.value()));
        }
        return simdjson::SUCCESS;
      }
      case simdjson::ondemand::json_type::number:
        return value.get_double().error();
      case simdjson::ondemand::json_type::string:
        return value.get_string(true).error();
      case simdjson::ondemand::json_type::boolean:
        return value.get_bool().error();
      case simdjson::ondemand::json_type::null: {
        SIMDJSON_ASSIGN_OR_RAISE(auto isNull, value.is_null());
        return isNull ? simdjson::SUCCESS : simdjson::N_ATOM_ERROR;
      }
    }
    BOLT_UNREACHABLE();
  }

  mutable folly::once_flag initializeErrors_;
  mutable std::exception_ptr errors_[simdjson::NUM_ERROR_CODES];
  // Padding is needed in case string view is inlined.
  mutable std::string paddedInput_;

  mutable folly::once_flag initUseSonic_;
  mutable bool useSonic_ = true;
};

// json_to_map(json) -> map<varchar, varchar>
// like CAST(JSON json AS MAP<VARCHAR, VARCHAR>)
class JsonToMapFunction : public bytedance::bolt::exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto castFactory = JsonCastOperator::get();
    castFactory->castFrom(*args[0], context, rows, outputType, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder()
                .returnType("map(varchar,varchar)")
                .argumentType("varchar")
                .build()};
  }
};
} // namespace

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_json_format,
    JsonFormatFunction::signatures(),
    std::make_unique<JsonFormatFunction>());

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_json_parse,
    JsonParseFunction::signatures(),
    [](const std::string& /*name*/,
       const std::vector<exec::VectorFunctionArg>&,
       const bolt::core::QueryConfig&) {
      return std::make_shared<JsonParseFunction>();
    });

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_json_to_map,
    JsonToMapFunction::signatures(),
    std::make_unique<JsonToMapFunction>());
} // namespace bytedance::bolt::functions
