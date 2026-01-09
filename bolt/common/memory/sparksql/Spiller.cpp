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

#include <cstdint>
#include <memory>
#include <set>

#include "bolt/common/memory/sparksql/NativeMemoryManagerFactory.h"
#include "bolt/common/memory/sparksql/Spiller.h"
#include "bolt/common/memory/sparksql/WeakPtrHelper.h"
namespace bytedance::bolt::memory::sparksql {

const std::set<SpillerPhase>& SpillerHelper::phaseSetAll() {
  static const std::set<SpillerPhase> kPhases{
      SpillerPhase::kShrink, SpillerPhase::kSpill};
  return kPhases;
}

const std::set<SpillerPhase>& SpillerHelper::phaseSetShrinkOnly() {
  static const std::set<SpillerPhase> kPhases{SpillerPhase::kShrink};
  return kPhases;
}

const std::set<SpillerPhase>& SpillerHelper::phaseSetSpillOnly() {
  static const std::set<SpillerPhase> kPhases{SpillerPhase::kSpill};
  return kPhases;
}

int64_t ShrinkSpiller::spill(MemoryTargetWeakPtr self, int64_t size) {
  return holder_->shrink(size);
}

const std::set<SpillerPhase>& ShrinkSpiller::applicablePhases() {
  return SpillerHelper::phaseSetShrinkOnly();
}

void ShrinkSpiller::setManagerHolder(BoltMemoryManagerHolder* holder) {
  holder_ = holder;
}

} // namespace bytedance::bolt::memory::sparksql
