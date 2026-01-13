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

#include <random>

#include "bolt/exec/ContainerRowSerde.h"
#include "bolt/exec/HybridSorter.h"
#include "bolt/exec/Operator.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/SortBuffer.h"
#include "bolt/exec/Spiller.h"
namespace bytedance::bolt::exec {
class LocalShuffle : public Operator {
 public:
  LocalShuffle(
      int32_t operatorId,
      DriverCtx* FOLLY_NONNULL driverCtx,
      const std::shared_ptr<const core::LocalShuffleNode>& localShuffleNode);

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* FOLLY_NULLABLE /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  void close() override;

 private:
  // Randomly shuffle rows in 'data_' and store the results in
  // 'shuffledRows_'.
  void shuffle();
  // Invoked to initialize or reset the reusable output buffer to get output.
  void prepareOutput();

  // Indicates data has been shuffled.
  bool shuffled_ = false;

  bool needsInput_ = true;

  // Indicates no more input. Once it is set, addInput() can't be called on this
  // sort buffer object.
  bool noMoreInput_ = false;

  // The max number of rows to output in each getOutput() call.
  uint32_t maxOutputRows_ = 0;

  // The seed of the random number generator.
  int64_t seed_ = 0;

  // For more randomness, we use a pool to store rows. Shuffle will be called
  // when the row number in data_ is greater than poolSize_.
  int64_t poolSize_ = 0;

  // Current rows that have been not been consumed by getOutput().
  int64_t currentRows_ = 0;

  // Indicates the offset of the current output rows in shuffledRows_.
  int64_t numCurrentOutputRows_ = 0;

  // Total row number in data_.
  int64_t numInputRows_ = 0;

  // Row container to store input rows.
  std::unique_ptr<RowContainer> data_;
  // Row pointers to the rows in data_ that have been shuffled.
  std::vector<char*> shuffledRows_;

  // Random number generator.
  std::mt19937_64 gen_;

  // Reusable output vector.
  RowVectorPtr output_;
};
} // namespace bytedance::bolt::exec
