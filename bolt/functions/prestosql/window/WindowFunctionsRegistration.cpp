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

#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "bolt/functions/lib/window/RegistrationFunctions.h"
namespace bytedance::bolt::window {

namespace prestosql {

extern void registerCumeDist(const std::string& name);
extern void registerNtileBigint(const std::string& name);
extern void registerFirstValue(const std::string& name);
extern void registerLastValue(const std::string& name);
extern void registerLag(const std::string& name);
extern void registerLead(const std::string& name);

void registerAllWindowFunctions(const std::string& prefix) {
  functions::window::registerRowNumberBigint(prefix + "row_number");
  functions::window::registerRankBigint(prefix + "rank");
  functions::window::registerDenseRankBigint(prefix + "dense_rank");
  functions::window::registerPercentRank(prefix + "percent_rank");
  registerCumeDist(prefix + "cume_dist");
  functions::window::registerNtileBigint(prefix + "ntile");
  functions::window::registerNthValueBigint(prefix + "nth_value");
  registerFirstValue(prefix + "first_value");
  registerLastValue(prefix + "last_value");
  registerLag(prefix + "lag");
  registerLead(prefix + "lead");
}

} // namespace prestosql

} // namespace bytedance::bolt::window
