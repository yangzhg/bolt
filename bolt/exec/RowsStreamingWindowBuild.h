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

#include "bolt/exec/Spiller.h"
#include "bolt/exec/WindowBuild.h"
namespace bytedance::bolt::exec {

/// Unlike StreamingWindowBuild, RowsStreamingWindowBuild is capable of
/// processing window functions as rows arrive within a single partition,
/// without the need to wait for the entire partition to be ready. This approach
/// can significantly reduce memory usage, especially when a single partition
/// contains a large amount of data. It is particularly suited for optimizing
/// rank and row_number functions, as well as aggregate window functions with a
/// default frame.
template <bool needSort>
class RowsStreamingWindowBuild : public WindowBuild {
 public:
  RowsStreamingWindowBuild(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      bolt::memory::MemoryPool* pool,
      const common::SpillConfig* spillConfig,
      tsan_atomic<bool>* nonReclaimableSection,
      uint64_t spillMemoryThreshold,
      bool enableJit);

  void addInput(RowVectorPtr input) override;

  void spill() override {
    // spillConfig_ has value, may reach here
    // but do nothing
  }

  void noMoreInput() override;

  bool hasNextPartition() override;

  std::shared_ptr<WindowPartition> nextPartition() override;

  bool needsInput() override {
    // No partitions are available or the currentPartition is the last available
    // one, so can consume input rows.
    return windowPartitions_.size() == 0 ||
        outputCurrentPartition_ == windowPartitions_.size() - 1;
  }

  bool hasOutputAll() override {
    return (
        (outputCurrentPartition_ >= windowPartitions_.size() - 1) &&
        finishLoadFromSpill_);
  }

  void setIgnorePeer(bool ignorePeer) override {
    ignorePeer_ = ignorePeer;
  }

  void loadNextPartialPartitionFromSpill() override {
    if (needSort && sortSpiller_) {
      loadNextPartitionFromSpill();
    }
  }

 private:
  void buildNextInputOrPartition(bool isFinished);

  void buildNextInputOrPartitionFromSpill(
      bool isFinished,
      bool patialPartitionFinished,
      char* allocatedStart,
      unsigned long allocatedSize);

  void buildPartition();

  char* storeRows();

  void loadNextPartitionFromSpill();

  void finish() override;

  // Holds input rows within the current partition.
  std::vector<char*> inputRows_;

  // Used to compare rows based on partitionKeys.
  char* previousRow_ = nullptr;

  // Current partition being output. Used to return the WidnowPartitions.
  vector_size_t outputCurrentPartition_ = -1;

  // Current partition when adding input. Used to construct WindowPartitions.
  vector_size_t inputCurrentPartition_ = 0;

  // Holds all the WindowPartitions.
  std::vector<std::shared_ptr<WindowPartition>> windowPartitions_;

  bool ignorePeer_{false};

  std::unique_ptr<Spiller> spiller_;
};

} // namespace bytedance::bolt::exec
