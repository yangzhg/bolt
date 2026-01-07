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

#include "bolt/exec/Aggregate.h"
#include "bolt/exec/fuzzer/AggregationFuzzerBase.h"
#include "bolt/exec/fuzzer/ReferenceQueryRunner.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
namespace bytedance::bolt::exec::test {

/// Runs the aggregation fuzzer.
/// @param signatureMap Map of all aggregate function signatures.
/// @param seed Random seed - Pass the same seed for reproducibility.
/// @param orderDependentFunctions Map of functions that depend on order of
/// input.
/// @param planPath Path to persisted plan information. If this is
/// supplied, fuzzer will only verify the plans.
/// @param referenceQueryRunner Reference query runner for results
/// verification.
void aggregateFuzzer(
    AggregateFunctionSignatureMap signatureMap,
    size_t seed,
    const std::unordered_map<std::string, std::shared_ptr<ResultVerifier>>&
        orderDependentFunctions,
    const std::unordered_map<std::string, std::shared_ptr<InputGenerator>>&
        customInputGenerators,
    VectorFuzzer::Options::TimestampPrecision timestampPrecision,
    const std::unordered_map<std::string, std::string>& queryConfigs,
    const std::optional<std::string>& planPath,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner);

} // namespace bytedance::bolt::exec::test
