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

#pragma once
namespace bytedance::bolt::aggregate {
const char* const kCPC = "cpc";
const char* const kHLL = "hll";
const char* const kHLLUnionSketches = "hll_union_sketches";
const char* const kKLL = "kll";
const char* const kFI = "fi";
const char* const kREQ = "req";
const char* const kTheta = "theta";
const char* const kQuantiles = "quantiles";
const char* const kVarOpt = "var_opt";

const char* const kCPCSketch = "cpc_sketch";
const char* const kHLLSketch = "hll_sketch";

const char* const kKLLSketchBool = "kll_sketch_b";
const char* const kKLLSketchChar = "kll_sketch_c";
const char* const kKLLSketchShort = "kll_sketch_s";
const char* const kKLLSketchInt = "kll_sketch_i";
const char* const kKLLSketchLong = "kll_sketch_l";
const char* const kKLLSketchFloat = "kll_sketch_f";
const char* const kKLLSketchDouble = "kll_sketch_d";

} // namespace bytedance::bolt::aggregate
namespace bytedance::bolt::functions {
const char* const kHLLEstimate = "hll_estimate";
} // namespace bytedance::bolt::functions
