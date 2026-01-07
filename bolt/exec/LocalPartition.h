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

#include "bolt/exec/Operator.h"
#include "bolt/exec/PushBasedEvent.h"
#include "bolt/exec/VectorHasher.h"
namespace bytedance::bolt::exec {

/// Keeps track of the total size in bytes of the data buffered in all
/// LocalExchangeQueues.
class LocalExchangeMemoryManager {
 public:
  explicit LocalExchangeMemoryManager(int64_t maxBufferSize)
      : maxBufferSize_{maxBufferSize} {}

  /// Returns 'true' if memory limit is reached or exceeded and sets future that
  /// will be complete when memory usage is update to be below the limit.
  bool increaseMemoryUsage(ContinueFuture* future, int64_t added);

  /// Decreases the memory usage by 'removed' bytes. If the memory usage goes
  /// below the limit after the decrease, the function returns 'promises_' to
  /// caller to fulfill.
  std::vector<ContinuePromise> decreaseMemoryUsage(int64_t removed);

  inline int64_t getMaxBufferSize() {
    return maxBufferSize_;
  }

  inline int64_t getCurBufferSize() {
    return bufferedBytes_;
  }

 private:
  const int64_t maxBufferSize_;
  std::mutex mutex_;
  int64_t bufferedBytes_{0};
  std::vector<ContinuePromise> promises_;
};

/// Buffers data for a single partition produced by local exchange. Allows
/// multiple producers to enqueue data and multiple consumers fetch data. Each
/// producer must be registered with a call to 'addProducer'. 'noMoreProducers'
/// must be called after all producers have been registered. A producer calls
/// 'enqueue' multiple time to put the data and calls 'noMoreData' when done.
/// Consumers call 'next' repeatedly to fetch the data.
class LocalExchangeQueue {
 public:
  LocalExchangeQueue(
      std::shared_ptr<LocalExchangeMemoryManager> memoryManager,
      int partition,
      bool morselSliced,
      int32_t morselSize,
      int32_t primedQueueSize)
      : memoryManager_{std::move(memoryManager)},
        partition_{partition},
        morselSliced_(morselSliced),
        morselSize_(morselSize),
        primedQueueSize_(primedQueueSize) {}

  // Create a LocalExchangeQueue that is filled with data. And no other producer
  // is going to write to this LocalExchangeQueue.
  LocalExchangeQueue(
      std::shared_ptr<LocalExchangeMemoryManager> memoryManager,
      int partition,
      folly::Synchronized<std::queue<RowVectorPtr>> queue)
      : memoryManager_{std::move(memoryManager)},
        partition_{partition},
        queue_{std::move(queue)},
        noMoreProducers_{true} {}

  LocalExchangeQueue(
      std::shared_ptr<LocalExchangeMemoryManager> memoryManager,
      int partition,
      bool morselSliced,
      int32_t morselSize,
      int32_t primedQueueSize,
      std::shared_ptr<PushBasedEvent> event)
      : memoryManager_{std::move(memoryManager)},
        partition_{partition},
        morselSliced_(morselSliced),
        morselSize_(morselSize),
        primedQueueSize_(primedQueueSize),
        driverDispatcher_(event) {}

  std::string toString() const {
    return fmt::format("LocalExchangeQueue({})", partition_);
  }

  uint32_t size() {
    return queue_.rlock()->size();
  }

  void addProducer();

  void noMoreProducers();

  /// Used by a producer to add data. Returning kNotBlocked if can accept more
  /// data. Otherwise returns kWaitForConsumer and sets future that will be
  /// completed when ready to accept more data.
  BlockingReason enqueue(RowVectorPtr input, ContinueFuture* future);

  /// Called by a producer to indicate that no more data will be added.
  void noMoreData();

  /// Used by a consumer to fetch some data. Returns kNotBlocked and sets data
  /// to nullptr if all data has been fetched and all producers are done
  /// producing data. Returns kWaitForProducer if there is no data, but some
  /// producers are not done producing data. Sets future that will be completed
  /// once there is data to fetch or if all producers report completion.
  ///
  /// @param pool Memory pool used to copy the data before returning.
  BlockingReason
  next(ContinueFuture* future, memory::MemoryPool* pool, RowVectorPtr* data);

  bool isFinished();

  /// Drop remaining data from the queue and notify consumers and producers if
  /// called before all the data has been processed. No-op otherwise.
  void close();

  void abort();

  std::shared_ptr<LocalExchangeQueue> spawnPrimedQueueLocked(
      std::queue<RowVectorPtr>& queue,
      uint64_t& reducedBytes,
      uint32_t maxSize = std::numeric_limits<uint32_t>::max()) {
    std::queue<RowVectorPtr> newQueue;
    while (!queue.empty() && newQueue.size() < maxSize) {
      reducedBytes += queue.front()->estimateFlatSize();
      newQueue.push(std::move(queue.front()));
      queue.pop();
    }

    auto memoryManager = std::make_shared<LocalExchangeMemoryManager>(
        memoryManager_->getMaxBufferSize());

    ContinueFuture future;
    if (!memoryManager->increaseMemoryUsage(&future, reducedBytes)) {
      VLOG(1) << "Spawning new LocalExchangeQueue of size "
              << memoryManager->getCurBufferSize() << " when maxBufferSize is "
              << memoryManager->getMaxBufferSize();
    }

    std::shared_ptr<LocalExchangeQueue> primedQueue =
        std::make_shared<LocalExchangeQueue>(
            memoryManager,
            partition_,
            folly::Synchronized<std::queue<RowVectorPtr>>{newQueue});
    return primedQueue;
  }

  int getPartition() {
    return partition_;
  }

  // [morsel-driven] notify LocalExchangeQueue to abort (stop enqueuing data).
  // Will not take effect if morsel-driven is disabled.
  bool setAbort() {
    if (!driverDispatcher_) {
      return false;
    }
    bool setAbort = aborted_.withWLock([&](auto& aborted) {
      if (!aborted) {
        aborted = true;
        abort();
        return true;
      }
      return false;
    });
    return setAbort;
  }

  // Check if LocalExchangeQueue has been aborted due to early completion
  FOLLY_ALWAYS_INLINE bool isAborted() {
    return *(aborted_.rlock());
  }

 private:
  bool isFinishedLocked(const std::queue<RowVectorPtr>& queue) const;

  std::shared_ptr<LocalExchangeMemoryManager> memoryManager_;
  const int partition_;
  folly::Synchronized<std::queue<RowVectorPtr>> queue_;
  // Satisfied when data becomes available or all producers report that they
  // finished producing, e.g. queue_ is not empty or noMoreProducers_ is true
  // and pendingProducers_ is zero.
  std::vector<ContinuePromise> consumerPromises_;
  int pendingProducers_{0};
  bool noMoreProducers_{false};
  bool closed_{false};

  // LocalExchangeQueue can be aborted due to early completion (usually when
  // skipProbeOnEmptyBuild in morsel-driven mode)
  folly::Synchronized<bool> aborted_{false};

  // Configs for morsel split
  bool morselSliced_{false};
  int32_t morselSize_;
  int32_t primedQueueSize_;

  // Driver dispatcher used to create & schedule drivers for consuming pipeline
  // in morsel-driven mode
  std::shared_ptr<PushBasedEvent> driverDispatcher_{nullptr};

  // A row vector cache to keep small size of morsel.
  RowVectorPtr lastUnmergedRowVector{nullptr};
};

/// Fetches data for a single partition produced by local exchange from
/// LocalExchangeQueue.
class LocalExchange : public SourceOperator {
 public:
  LocalExchange(
      int32_t operatorId,
      DriverCtx* ctx,
      RowTypePtr outputType,
      const std::string& planNodeId,
      int partition,
      std::shared_ptr<LocalExchangeQueue> primedQueue = nullptr);

  std::string toString() const override {
    return fmt::format("LocalExchange({})", partition_);
  }

  BlockingReason isBlocked(ContinueFuture* future) override;

  RowVectorPtr getOutput() override;

  bool isFinished() override;

  /// Close exchange queue. If called before all data has been processed,
  /// notifies the producer that no more data is needed.
  void close() override {
    Operator::close();
    if (queue_) {
      queue_->close();
    }
  }

 private:
  const int partition_;
  const std::shared_ptr<LocalExchangeQueue> queue_{nullptr};
  ContinueFuture future_;
  BlockingReason blockingReason_{BlockingReason::kNotBlocked};
};

/// Hash partitions the data using specified keys. The number of partitions is
/// determined by the number of LocalExchangeQueues(s) found in the task.
class LocalPartition : public Operator {
 public:
  LocalPartition(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const core::LocalPartitionNode>& planNode);

  LocalPartition(
      int32_t operatorId,
      size_t numPartitions,
      DriverCtx* ctx,
      const std::shared_ptr<const core::LocalPartitionNode>& planNode);

  std::string toString() const override {
    return fmt::format("LocalPartition({})", numPartitions_);
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  // Always true but the caller will check isBlocked before adding input, hence
  // the blocked state does not accumulate input.
  bool needsInput() const override {
    return true;
  }

  BlockingReason isBlocked(ContinueFuture* future) override;

  void noMoreInput() override;

  bool isFinished() override;

 private:
  // [morsel] can notify peer LocalPartition operators in the same pipeline to
  // terminate
  void notifyPeersToTerminate();

  const std::vector<std::shared_ptr<LocalExchangeQueue>> queues_;
  const size_t numPartitions_;
  std::unique_ptr<core::PartitionFunction> partitionFunction_;

  std::vector<BlockingReason> blockingReasons_;
  std::vector<ContinueFuture> futures_;

  /// Reusable memory for hash calculation.
  std::vector<uint32_t> partitions_;
};

} // namespace bytedance::bolt::exec
