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

#include "PythonScalarFunction.h"
#include "bolt/python/Utils.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::py {

namespace py = pybind11;

template <TypeKind kind>
py::object getPyObjectFromVector(
    const VectorPtr& vec,
    const vector_size_t idx) {
  BOLT_CHECK(idx >= 0 && idx < vec->size(), "Index out of range");

  if (vec->isNullAt(idx)) {
    return py::none();
  }

  using T = typename TypeTraits<kind>::NativeType;
  if constexpr (std::is_same_v<T, bytedance::bolt::StringView>) {
    const bytedance::bolt::StringView value =
        vec->as<SimpleVector<bytedance::bolt::StringView>>()->valueAt(idx);
    py::str result = std::string_view(value);
    return result;
  }

  py::object result = bolt::python::pyTry<py::object>(
      [&]() { return py::cast(vec->as<SimpleVector<T>>()->valueAt(idx)); });

  return result;
}

template <TypeKind kind>
void setPyObjectInFlatVector(
    const VectorPtr& vec,
    const vector_size_t idx,
    py::object& result) {
  BOLT_CHECK(idx >= 0 && idx < vec->size(), "Index out of range");

  if (result.is_none()) {
    return vec->setNull(idx, true);
  }

  using T = typename TypeTraits<kind>::NativeType;
  auto flatResult = vec->asFlatVector<T>();

  flatResult->set(idx, result.cast<T>());
}

class UserDefinedPythonScalarFunction : public exec::VectorFunction {
 public:
  explicit UserDefinedPythonScalarFunction(
      int numArgs,
      pybind11::function callback,
      TypePtr returnType)
      : numArgs_(numArgs),
        function_(std::move(callback)),
        returnType_(returnType) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BOLT_CHECK(numArgs_ >= args.size());
    for (auto& arg : args) {
      // The argument may be flat or constant.
      BOLT_CHECK(arg->isFlatEncoding() || arg->isConstantEncoding());
    }

    BaseVector::ensureWritable(rows, returnType_, context.pool(), result);
    result->clearNulls(rows);

    rows.applyToSelected([&](auto row) {
      py::list pyArgs;
      for (int i = 0; i < args.size(); ++i) {
        auto& arg = args[i];
        py::object val = py::none();
        if (arg->isConstantEncoding()) {
          val = BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
              getPyObjectFromVector, arg->typeKind(), arg, 0);
        } else {
          val = BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
              getPyObjectFromVector, arg->typeKind(), arg, row);
        }
        pyArgs.append(val);
      }

      py::object res =
          bolt::python::pyTry<py::object>([&]() { return function_(*pyArgs); });

      BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
          setPyObjectInFlatVector, result->typeKind(), result, row, res);
    });
  }

 private:
  int numArgs_;
  pybind11::function function_;
  TypePtr returnType_;
};

void registerPythonScalarFunction(
    pybind11::function callback,
    std::string alias,
    TypePtr returnType,
    int numArgs) {
  bolt::exec::registerVectorFunction(
      alias,
      bolt::python::getSignatures(returnType),
      std::make_unique<UserDefinedPythonScalarFunction>(
          numArgs, std::move(callback), returnType));
}
} // namespace bytedance::bolt::py
