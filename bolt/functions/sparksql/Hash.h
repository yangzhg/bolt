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
namespace bytedance::bolt::functions::sparksql {

// Supported types:
//   - Boolean
//   - Integer types (tinyint, smallint, integer, bigint)
//   - Varchar, varbinary
//   - Real, double
//   - Decimal
//   - Date
//   - Timestamp
//
// TODO:
//   - Row, Array: hash the elements in order
//   - Map: iterate over map, hashing key then value. Since map ordering is
//        unspecified, hashing logically equivalent maps may result in
//        different hash values.

std::vector<std::shared_ptr<exec::FunctionSignature>> hashSignatures();

std::vector<std::shared_ptr<exec::FunctionSignature>> hashWithSeedSignatures();

std::shared_ptr<exec::VectorFunction> makeHash(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::shared_ptr<exec::VectorFunction> makeHashWithSeed(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

// Supported types:
//   - Bools
//   - Integer types (byte, short, int, long)
//   - String, Binary
//   - Float, Double
//   - Decimal
//   - Date
//   - Timestamp
//
// Unsupported:
//   - Structs, Arrays: hash the elements in order
//   - Maps: iterate over map, hashing key then value. Since map ordering is
//        unspecified, hashing logically equivalent maps may result in
//        different hash values.

std::vector<std::shared_ptr<exec::FunctionSignature>> xxhash64Signatures();

std::vector<std::shared_ptr<exec::FunctionSignature>>
xxhash64WithSeedSignatures();

std::shared_ptr<exec::VectorFunction> makeXxHash64(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

std::shared_ptr<exec::VectorFunction> makeXxHash64WithSeed(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

// Supported types:
//   - Boolean
//   - Integer types (tinyint, smallint, integer, bigint)
//   - Varchar, varbinary
//   - Real, double
//   - ShortDecimal
//   - Date, Timestamp
//
// TODO:
//   - LongDecimal, Row, Array, Map

std::vector<std::shared_ptr<exec::FunctionSignature>> hiveHashSignatures();

std::shared_ptr<exec::VectorFunction> makeHiveHash(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

} // namespace bytedance::bolt::functions::sparksql
