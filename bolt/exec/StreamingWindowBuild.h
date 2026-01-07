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

#include "bolt/exec/WindowBuild.h"
namespace bytedance::bolt::exec {

/// The StreamingWindowBuild is used when the input data is already sorted by
/// {partition keys + order by keys}. The logic identifies partition changes
/// when receiving input rows and splits out WindowPartitions for the Window
/// operator to process.
class StreamingWindowBuild : public WindowBuild {
 public:
  StreamingWindowBuild(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      bolt::memory::MemoryPool* pool,
      const common::SpillConfig* spillConfig,
      tsan_atomic<bool>* nonReclaimableSection,
      uint64_t spillMemoryThreshold,
      bool enableJit);

  void addInput(RowVectorPtr input) override;

  void spill() override {}

  void noMoreInput() override;

  bool hasNextPartition() override;

  std::shared_ptr<WindowPartition> nextPartition() override;

  bool needsInput() override {
    // No partitions are available or the currentPartition is the last available
    // one, so can consume input rows.
    return partitionStartRows_.size() == 0 ||
        currentPartition_ == partitionStartRows_.size() - 2;
  }

  bool hasOutputAll() override {
    // No partitions are available or the currentPartition is the last available
    // one, so can consume input rows.
    return currentPartition_ >= partitionStartRows_.size() - 2;
  }

  uint64_t* getBuildPartitionTime() override {
    return &buildPartitionTimeUs_;
  }

  uint64_t* getloadFromSpillTime() override {
    return &loadFromSpillTimeUs_;
  }

 private:
  void buildNextPartition();

  // Vector of pointers to each input row in the data_ RowContainer.
  // Rows are erased from data_ when they are output from the
  // Window operator.
  std::vector<char*> sortedRows_;

  // Holds input rows within the current partition.
  std::vector<char*> inputRows_;

  // Indices of  the start row (in sortedRows_) of each partition in
  // the RowContainer data_. This auxiliary structure helps demarcate
  // partitions.
  std::vector<vector_size_t> partitionStartRows_;

  // Used to compare rows based on partitionKeys.
  char* previousRow_ = nullptr;

  // Current partition being output. Used to construct WindowPartitions
  // during resetPartition.
  vector_size_t currentPartition_ = -1;

  uint64_t loadFromSpillTimeUs_ = 0;

  uint64_t buildPartitionTimeUs_{0};
};

} // namespace bytedance::bolt::exec
