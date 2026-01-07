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

#include "bolt/functions/sketches/RegistrationAggregateFunctions.h"
namespace bytedance::bolt::aggregate {
extern void registerSketchCPCAggregate();
extern void registerCPCSketchAggregate();
extern void registerSketchFIAggregate();
extern void registerHLLAggregate();
extern void registerHLLSketchAggregate();
extern void registerHLLUnionSketchesAggregate();
extern void registerSketchKLLAggregate();
extern void registerKLLSketchesAggregate();
extern void registerSketchQuantilesAggregate();
extern void registerSketchREQAggregate();
extern void registerSketchThetaAggregate();
extern void registerSketchVarOptAggregate();
namespace sketches {
void registerAllAggregateFunctions() {
  registerSketchCPCAggregate();
  registerCPCSketchAggregate();
  registerSketchFIAggregate();
  registerHLLAggregate();
  registerHLLSketchAggregate();
  registerHLLUnionSketchesAggregate();
  registerSketchKLLAggregate();
  registerKLLSketchesAggregate();
  registerSketchQuantilesAggregate();
  registerSketchREQAggregate();
  registerSketchThetaAggregate();
  registerSketchVarOptAggregate();
}
} // namespace sketches
} // namespace bytedance::bolt::aggregate