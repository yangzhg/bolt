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

#include "bolt/common/memory/ArbitrationOperation.h"
#include <mutex>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/RuntimeMetrics.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/testutil/TestValue.h"
#include "bolt/common/time/Timer.h"

using bytedance::bolt::common::testutil::TestValue;
namespace bytedance::bolt::memory {
using namespace bytedance::bolt::memory;

ArbitrationOperation::ArbitrationOperation(
    ScopedArbitrationParticipant&& participant,
    uint64_t requestBytes,
    uint64_t timeoutNs)
    : requestBytes_(requestBytes),
      timeoutNs_(timeoutNs),
      createTimeNs_(getCurrentTimeNano()),
      participant_(std::move(participant)) {
  BOLT_CHECK_GT(requestBytes_, 0);
}

ArbitrationOperation::~ArbitrationOperation() {
  BOLT_CHECK_NE(
      state_,
      State::kRunning,
      "Unexpected arbitration operation state on destruction");
}

std::string ArbitrationOperation::stateName(State state) {
  switch (state) {
    case State::kInit:
      return "init";
    case State::kWaiting:
      return "waiting";
    case State::kRunning:
      return "running";
    case State::kFinished:
      return "finished";
    default:
      return fmt::format("unknown state: {}", static_cast<int>(state));
  }
}

void ArbitrationOperation::setState(State state) {
  switch (state) {
    case State::kWaiting:
      BOLT_CHECK_EQ(state_, State::kInit);
      break;
    case State::kRunning:
      BOLT_CHECK(this->state_ == State::kWaiting || state_ == State::kInit);
      break;
    case State::kFinished:
      BOLT_CHECK_EQ(this->state_, State::kRunning);
      break;
    default:
      BOLT_UNREACHABLE(
          "Unexpected state transition from {} to {}", state_, state);
      break;
  }
  state_ = state;
}

void ArbitrationOperation::start() {
  BOLT_CHECK_EQ(state_, State::kInit);
  participant_->startArbitration(this);
  setState(ArbitrationOperation::State::kRunning);
  BOLT_CHECK_EQ(startTimeNs_, 0);
  startTimeNs_ = getCurrentTimeNano();
}

void ArbitrationOperation::finish() {
  setState(State::kFinished);
  BOLT_CHECK_EQ(finishTimeNs_, 0);
  finishTimeNs_ = getCurrentTimeNano();
  participant_->finishArbitration(this);
}

bool ArbitrationOperation::aborted() const {
  return participant_->aborted();
}

uint64_t ArbitrationOperation::executionTimeNs() const {
  if (state_ == State::kFinished) {
    BOLT_CHECK_GE(finishTimeNs_, createTimeNs_);
    return finishTimeNs_ - createTimeNs_;
  } else {
    const auto currentTimeNs = getCurrentTimeNano();
    BOLT_CHECK_GE(currentTimeNs, createTimeNs_);
    return currentTimeNs - createTimeNs_;
  }
}

bool ArbitrationOperation::hasTimeout() const {
  return state_ != State::kFinished && timeoutNs() <= 0;
}

uint64_t ArbitrationOperation::timeoutNs() const {
  if (state_ == State::kFinished) {
    return 0;
  }
  const auto execTimeNs = executionTimeNs();
  if (execTimeNs >= timeoutNs_) {
    return 0;
  }
  return timeoutNs_ - execTimeNs;
}

void ArbitrationOperation::setGrowTargets() {
  // We shall only set grow targets once after start execution.
  BOLT_CHECK_EQ(state_, State::kRunning);
  BOLT_CHECK(
      maxGrowBytes_ == 0 && minGrowBytes_ == 0,
      "Arbitration operation grow targets have already been set: {}/{}",
      succinctBytes(maxGrowBytes_),
      succinctBytes(minGrowBytes_));
  participant_->getGrowTargets(requestBytes_, maxGrowBytes_, minGrowBytes_);
  BOLT_CHECK_LE(requestBytes_, maxGrowBytes_);
}

ArbitrationOperation::Stats ArbitrationOperation::stats() const {
  BOLT_CHECK_EQ(state_, State::kFinished);
  BOLT_CHECK_NE(startTimeNs_, 0);

  const uint64_t executionTimeNs = this->executionTimeNs();

  BOLT_CHECK_GE(startTimeNs_, createTimeNs_);
  const uint64_t localArbitrationWaitTimeNs = startTimeNs_ - createTimeNs_;
  if (globalArbitrationStartTimeNs_ == 0) {
    return {
        localArbitrationWaitTimeNs,
        finishTimeNs_ - startTimeNs_,
        0,
        executionTimeNs};
  }

  BOLT_CHECK_GE(globalArbitrationStartTimeNs_, startTimeNs_);
  const uint64_t localArbitrationExecTimeNs =
      globalArbitrationStartTimeNs_ - startTimeNs_;
  return {
      localArbitrationWaitTimeNs,
      localArbitrationExecTimeNs,
      finishTimeNs_ - globalArbitrationStartTimeNs_,
      executionTimeNs};
}

std::ostream& operator<<(std::ostream& out, ArbitrationOperation::State state) {
  out << ArbitrationOperation::stateName(state);
  return out;
}
} // namespace bytedance::bolt::memory
