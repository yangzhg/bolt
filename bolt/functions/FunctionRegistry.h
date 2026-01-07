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

#pragma once

#include <string>
#include <vector>

#include "bolt/expression/FunctionSignature.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt {

using FunctionSignatureMap = std::unordered_map<
    std::string,
    std::vector<std::shared_ptr<const exec::FunctionSignature>>>;
/// Returns a mapping of all Simple and Vector functions registered in Bolt
/// The mapping is function name -> list of function signatures
FunctionSignatureMap getFunctionSignatures();

/// Given a function name and argument types, returns
/// the return type if function exists otherwise returns nullptr
std::shared_ptr<const Type> resolveFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes);

/// Given a function name and argument types, returns the return type if the
/// function exists or is a special form that supports type resolution (see
/// resolveCallableSpecialForm), otherwise returns nullptr.
std::shared_ptr<const Type> resolveFunctionOrCallableSpecialForm(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes);

/// Given the name of a special form and argument types, returns
/// the return type if the special form exists and is supported, otherwise
/// returns nullptr.
/// Special forms are not supported by this function if:
/// 1) they cannot be invoked as a CallExpr, e.g. FieldReference.
/// or
/// 2) their return types cannot be inferred from their argument types, e.g.
///    Cast.
std::shared_ptr<const Type> resolveCallableSpecialForm(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes);

/// Given name of simple function and argument types, returns
/// the return type if function exists otherwise returns nullptr
std::shared_ptr<const Type> resolveSimpleFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes);

/// Given name of vector function and argument types, returns
/// the return type if function exists otherwise returns nullptr
std::shared_ptr<const Type> resolveVectorFunction(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes);

/// Clears the function registry.
void clearFunctionRegistry();
} // namespace bytedance::bolt
