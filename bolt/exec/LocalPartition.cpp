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

#include "bolt/exec/LocalPartition.h"
#include "bolt/exec/Task.h"
namespace bytedance::bolt::exec {
namespace {
void notify(std::vector<ContinuePromise>& promises) {
  for (auto& promise : promises) {
    promise.setValue();
  }
}
} // namespace

bool LocalExchangeMemoryManager::increaseMemoryUsage(
    ContinueFuture* future,
    int64_t added) {
  std::lock_guard<std::mutex> l(mutex_);
  bufferedBytes_ += added;

  if (bufferedBytes_ >= maxBufferSize_) {
    promises_.emplace_back("LocalExchangeMemoryManager::updateMemoryUsage");
    *future = promises_.back().getSemiFuture();
    return true;
  }

  return false;
}

std::vector<ContinuePromise> LocalExchangeMemoryManager::decreaseMemoryUsage(
    int64_t removed) {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    bufferedBytes_ -= removed;

    if (bufferedBytes_ < maxBufferSize_) {
      promises = std::move(promises_);
    }
  }
  return promises;
}

void LocalExchangeQueue::addProducer() {
  queue_.withWLock([&](auto& /*queue*/) {
    BOLT_CHECK(!noMoreProducers_, "addProducer called after noMoreProducers");
    ++pendingProducers_;
  });
}

void LocalExchangeQueue::noMoreProducers() {
  std::vector<ContinuePromise> consumerPromises;
  queue_.withWLock([&](auto& queue) {
    BOLT_CHECK(!noMoreProducers_, "noMoreProducers can be called only once");
    noMoreProducers_ = true;
    if (pendingProducers_ == 0) {
      // optionally notify the PartitionedOutputBuffer of the task one creator
      // is finished
      if (driverDispatcher_) {
        driverDispatcher_->finish();
      }

      // No more data will be produced.
      consumerPromises = std::move(consumerPromises_);
    }
  });
  notify(consumerPromises);
}

BlockingReason LocalExchangeQueue::enqueue(
    RowVectorPtr input,
    ContinueFuture* future) {
  auto abort = aborted_.rlock();
  if (*abort) {
    // [morsel] LocalExchangeQueue has been aborted, we need to notify the
    // LocalPartition operator to stop fueling the queue with more data.
    BOLT_CHECK_NOT_NULL(
        driverDispatcher_,
        "LocalExchangeQueue cannot be aborted when it does not have 'morsel-driven' driverDispatcher");
    return BlockingReason::kYield;
  }

  std::vector<RowVectorPtr> morsels;

  if (morselSliced_ && input->size() > morselSize_) {
    BOLT_CHECK_GE(
        morselSize_,
        0,
        "Morsel size cannot be < 1. Current morselSize={}",
        morselSize_);
    for (auto offset = 0; offset < input->size(); offset += morselSize_) {
      auto length = std::min(input->size() - offset, morselSize_);
      auto sliced =
          std::dynamic_pointer_cast<RowVector>(input->slice(offset, length));
      morsels.emplace_back(std::move(sliced));
    }
  }
  // Enqueue last piece enqueue
  else if (input->size() > 0) {
    morsels.emplace_back(std::move(input));
  }

  std::vector<ContinuePromise> consumerPromises;
  std::vector<ContinuePromise> producerPromises;

  bool blockedOnConsumer = false;
  bool isClosed = queue_.withWLock([&](auto& queue) {
    if (closed_) {
      return true;
    }

    uint64_t inputBytesActual = 0;
    for (auto morsel : morsels) {
      inputBytesActual += morsel->estimateFlatSize();
      queue.push(std::move(morsel));
    }
    consumerPromises = std::move(consumerPromises_);

    // When size reaches the limit, make the current LocalExchangeQueue
    // spawn one or more child queue named "primedQueue" and then start a driver
    // for the consuming pipeline reading from each queue.
    if (driverDispatcher_) {
      BOLT_CHECK_GE(
          primedQueueSize_,
          0,
          "Primed queue size cannot be < 1. Current primedQueueSize_={}",
          primedQueueSize_);
      uint64_t reducedBytes = 0;
      while (queue.size() > primedQueueSize_) {
        BOLT_CHECK(
            consumerPromises.empty(),
            "If consuming pipeline is morsel-driven, the consumer promise should be empty.");
        std::shared_ptr<LocalExchangeQueue> primedQueue =
            spawnPrimedQueueLocked(queue, reducedBytes, primedQueueSize_);
        if (!driverDispatcher_->schedule(primedQueue, partition_)) {
          VLOG(1) << "Dispatcher already finished. No new driver created.";
        }
      }
      // [morsel-driven] morsel-driven mode should not be blocked
      // by memoryManager (otherwise, it might hang). As a result, we do not
      // update memory manager here.
    } else {
      if (memoryManager_->increaseMemoryUsage(
              future, (int64_t)inputBytesActual)) {
        blockedOnConsumer = true;
      }
    }

    return false;
  });

  // [morsel-driven]
  if (driverDispatcher_) {
    BOLT_CHECK(
        consumerPromises.empty(),
        "The consumer promises is not empty. This is unexpected for morsel-driven consuming pipeline that has not been started.");
  }

  if (isClosed) {
    return BlockingReason::kNotBlocked;
  }

  notify(consumerPromises);

  // [morsel-driven]
  notify(producerPromises);

  if (blockedOnConsumer) {
    return BlockingReason::kWaitForConsumer;
  }

  return BlockingReason::kNotBlocked;
}

void LocalExchangeQueue::noMoreData() {
  auto abort = aborted_.rlock();
  if (*abort) {
    return;
  }

  std::vector<ContinuePromise> consumerPromises;
  queue_.withWLock([&](auto& queue) {
    BOLT_CHECK_GT(pendingProducers_, 0);
    --pendingProducers_;
    if (noMoreProducers_ && pendingProducers_ == 0) {
      // [morsel-driven] optionally notify the PartitionedOutputBuffer of the
      // task one creator is finished
      // Start the last driver using all remaining data chunks in the queue,
      // even if the queue is empty because if the last morsel-driven driver
      // finishes before the last driver creator finishes, the query might hang
      // because Task::allPeersFinished will return false and since there is no
      // more new driver, then the HashTable will never finish.
      if (driverDispatcher_) {
        uint64_t reducedBytes = 0;
        std::shared_ptr<LocalExchangeQueue> primedQueue =
            spawnPrimedQueueLocked(queue, reducedBytes);
        if (driverDispatcher_->schedule(primedQueue, partition_, true)) {
          memoryManager_->decreaseMemoryUsage(reducedBytes);
        } else {
          VLOG(1) << "Dispatcher already finished. No new driver created.";
        }
      }

      consumerPromises = std::move(consumerPromises_);
    }
  });
  notify(consumerPromises);
}

BlockingReason LocalExchangeQueue::next(
    ContinueFuture* future,
    memory::MemoryPool* pool,
    RowVectorPtr* data) {
  std::vector<ContinuePromise> memoryPromises;
  auto blockingReason = queue_.withWLock([&](auto& queue) {
    *data = nullptr;
    if (queue.empty()) {
      if (isFinishedLocked(queue)) {
        return BlockingReason::kNotBlocked;
      }

      consumerPromises_.emplace_back("LocalExchangeQueue::next");
      *future = consumerPromises_.back().getSemiFuture();

      return BlockingReason::kWaitForProducer;
    }
    *data = queue.front();
    queue.pop();
    memoryPromises =
        memoryManager_->decreaseMemoryUsage((*data)->estimateFlatSize());

    return BlockingReason::kNotBlocked;
  });
  notify(memoryPromises);
  return blockingReason;
}

bool LocalExchangeQueue::isFinishedLocked(
    const std::queue<RowVectorPtr>& queue) const {
  if (closed_) {
    return true;
  }

  if (noMoreProducers_ && pendingProducers_ == 0 && queue.empty()) {
    return true;
  }

  return false;
}

bool LocalExchangeQueue::isFinished() {
  return queue_.withWLock([&](auto& queue) { return isFinishedLocked(queue); });
}

void LocalExchangeQueue::abort() {
  std::vector<ContinuePromise> consumerPromises;
  std::vector<ContinuePromise> producerPromises;
  queue_.withWLock([&](auto queue) {
    // Start the last driver using all remaining data chunks in the queue, even
    // if the queue is empty because if the last morsel-driven driver finishes
    // before the last driver creator finishes, the query might hang because
    // Task::allPeersFinished will return false and since there is no more new
    // driver, then the HashTable will never finish.
    // Also, notify the PartitionedOutputBuffer of the task one creator
    // is finished
    if (driverDispatcher_) {
      uint64_t reducedBytes = 0;
      std::shared_ptr<LocalExchangeQueue> primedQueue =
          spawnPrimedQueueLocked(queue, reducedBytes);
      if (driverDispatcher_->schedule(primedQueue, partition_, true)) {
        memoryManager_->decreaseMemoryUsage(reducedBytes);
      } else {
        VLOG(1) << "Dispatcher already finished. No new driver created.";
      }
      consumerPromises = std::move(consumerPromises_);
    }
  });
  notify(consumerPromises);
  notify(producerPromises);

  close();
}

void LocalExchangeQueue::close() {
  std::vector<ContinuePromise> consumerPromises;
  std::vector<ContinuePromise> memoryPromises;
  queue_.withWLock([&](auto& queue) {
    uint64_t freedBytes = 0;

    while (!queue.empty()) {
      freedBytes += queue.front()->estimateFlatSize();
      queue.pop();
    }

    if (freedBytes) {
      memoryPromises = memoryManager_->decreaseMemoryUsage(freedBytes);
    }

    consumerPromises = std::move(consumerPromises_);
    closed_ = true;
  });
  notify(consumerPromises);
  notify(memoryPromises);
}

LocalExchange::LocalExchange(
    int32_t operatorId,
    DriverCtx* ctx,
    RowTypePtr outputType,
    const std::string& planNodeId,
    int partition,
    std::shared_ptr<LocalExchangeQueue> primedQueue)
    : SourceOperator(
          ctx,
          std::move(outputType),
          operatorId,
          planNodeId,
          "LocalExchange"),
      partition_{partition},
      queue_{
          primedQueue == nullptr ? operatorCtx_->task()->getLocalExchangeQueue(
                                       ctx->splitGroupId,
                                       planNodeId,
                                       partition)
                                 : primedQueue} {
  BOLT_CHECK(
      queue_->getPartition() == partition,
      "Partition must be same for LocalExchange and prime q -uueue");
}

BlockingReason LocalExchange::isBlocked(ContinueFuture* future) {
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    *future = std::move(future_);
    auto reason = blockingReason_;
    blockingReason_ = BlockingReason::kNotBlocked;
    return reason;
  }

  return BlockingReason::kNotBlocked;
}

RowVectorPtr LocalExchange::getOutput() {
  RowVectorPtr data;
  blockingReason_ = queue_->next(&future_, pool(), &data);
  if (blockingReason_ != BlockingReason::kNotBlocked) {
    return nullptr;
  }
  if (data != nullptr) {
    auto lockedStats = stats_.wlock();
    lockedStats->addInputVector(data->estimateFlatSize(), data->size());
  }
  return data;
}

bool LocalExchange::isFinished() {
  return queue_->isFinished();
}

LocalPartition::LocalPartition(
    int32_t operatorId,
    DriverCtx* ctx,
    const std::shared_ptr<const core::LocalPartitionNode>& planNode)
    : Operator(
          ctx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "LocalPartition"),
      queues_{
          ctx->task->getLocalExchangeQueues(ctx->splitGroupId, planNode->id())},
      numPartitions_{queues_.size()},
      partitionFunction_(
          numPartitions_ == 1
              ? nullptr
              : planNode->partitionFunctionSpec().create(numPartitions_)) {
  BOLT_CHECK(numPartitions_ == 1 || partitionFunction_ != nullptr);

  for (auto& queue : queues_) {
    queue->addProducer();
  }
}

LocalPartition::LocalPartition(
    int32_t operatorId,
    size_t numPartitions,
    DriverCtx* ctx,
    const std::shared_ptr<const core::LocalPartitionNode>& planNode)
    : Operator(
          ctx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          "LocalPartition"),
      queues_{
          ctx->task->getLocalExchangeQueues(ctx->splitGroupId, planNode->id())},
      numPartitions_{numPartitions},
      partitionFunction_(
          numPartitions_ == 1
              ? nullptr
              : planNode->partitionFunctionSpec().create(numPartitions_)) {
  BOLT_CHECK(numPartitions_ == 1 || partitionFunction_ != nullptr);
  // Cannot have more partitions than queues.
  BOLT_CHECK(
      numPartitions_ <= queues_.size(),
      "Number of queues (drivers) must be bigger than or equal to the number of partitions.");

  for (auto& queue : queues_) {
    queue->addProducer();
  }
}

namespace {
std::vector<BufferPtr> allocateIndexBuffers(
    const std::vector<vector_size_t>& sizes,
    memory::MemoryPool* pool) {
  std::vector<BufferPtr> indexBuffers;
  indexBuffers.reserve(sizes.size());
  for (auto size : sizes) {
    indexBuffers.push_back(allocateIndices(size, pool));
  }
  return indexBuffers;
}

std::vector<vector_size_t*> getRawIndices(
    const BufferPtr& indexBuffer,
    const std::vector<vector_size_t>& sizes) {
  std::vector<vector_size_t*> rawIndices;
  rawIndices.reserve(sizes.size());
  auto data = indexBuffer->asMutable<vector_size_t>();
  vector_size_t offset = 0;
  for (auto size : sizes) {
    rawIndices.emplace_back(offset + data);
    offset += size;
  }
  return rawIndices;
}

RowVectorPtr
wrapChildren(const RowVectorPtr& input, vector_size_t size, BufferPtr indices) {
  std::vector<VectorPtr> wrappedChildren;
  wrappedChildren.reserve(input->type()->size());
  for (auto i = 0; i < input->type()->size(); i++) {
    wrappedChildren.emplace_back(BaseVector::wrapInDictionary(
        BufferPtr(nullptr), indices, size, input->childAt(i)));
  }

  return std::make_shared<RowVector>(
      input->pool(), input->type(), BufferPtr(nullptr), size, wrappedChildren);
}
} // namespace

void LocalPartition::notifyPeersToTerminate() {
  auto* driver = operatorCtx_->driver();
  BOLT_CHECK_NOT_NULL(driver);
  BOLT_CHECK(
      driver->driverCtx()->queryConfig().morselDrivenEnabled(),
      "LocalPartition should not notify peers to terminate in non morsel-driven mode");
  const auto& task = driver->task();
  const std::vector<Operator*> operators =
      task->findPeerOperators(operatorCtx_->driverCtx()->pipelineId, this);
  // Inform all peers to
  for (auto* op : operators) {
    op->noMoreInput();
  }
  LOG(ERROR)
      << "[morsel] Propagating skipProbeOnEmptyBuild to producing pipeline task="
      << this->taskId()
      << " pipeline=" << this->operatorCtx_->driverCtx()->pipelineId;
}

void LocalPartition::addInput(RowVectorPtr input) {
  {
    auto lockedStats = stats_.wlock();
    lockedStats->addOutputVector(input->estimateFlatSize(), input->size());
  }

  // Lazy vectors must be loaded or processed.
  for (auto& child : input->children()) {
    child->loadedVector();
  }

  if (numPartitions_ == 1) {
    ContinueFuture future;
    auto blockingReason = queues_[0]->enqueue(input, &future);
    if (FOLLY_UNLIKELY(blockingReason == BlockingReason::kYield)) {
      // [morsel] when enqueue to any localExchangeQueue returns yield, it means
      // the LocalExchangeQueues have been aborted (possibly due to
      // skipProbeOnEmptyBuild, in this case, we need to issue noMoreInput to
      // halt the driver.

      // @zj: find the correct way to yield producing pipeline when consuming
      // pipeline is aborted in skipProbeOnEmptyBuild
      //      notifyPeersToTerminate();
      //      noMoreInput();
    } else if (blockingReason != BlockingReason::kNotBlocked) {
      blockingReasons_.push_back(blockingReason);
      futures_.push_back(std::move(future));
    }
    return;
  }

  const auto singlePartition =
      partitionFunction_->partition(*input, partitions_);
  if (singlePartition.has_value()) {
    ContinueFuture future;
    auto blockingReason =
        queues_[singlePartition.value()]->enqueue(input, &future);
    if (FOLLY_UNLIKELY(blockingReason == BlockingReason::kYield)) {
      // [morsel] when enqueue to any localExchangeQueue returns yield, it means
      // the LocalExchangeQueues have been aborted (possibly due to
      // skipProbeOnEmptyBuild, in this case, we need to issue noMoreInput to
      // halt the driver.

      // @zj: find the correct way to yield producing pipeline when consuming
      // pipeline is aborted in skipProbeOnEmptyBuild
      //      notifyPeersToTerminate();
      //      noMoreInput();
    } else if (blockingReason != BlockingReason::kNotBlocked) {
      blockingReasons_.push_back(blockingReason);
      futures_.push_back(std::move(future));
    }
    return;
  }

  const auto numInput = input->size();
  std::vector<vector_size_t> maxIndex(numPartitions_, 0);
  for (auto i = 0; i < numInput; ++i) {
    ++maxIndex[partitions_[i]];
  }
  auto indexBuffer = allocateIndices(numInput, pool());
  auto rawIndices = getRawIndices(indexBuffer, maxIndex);

  std::fill(maxIndex.begin(), maxIndex.end(), 0);
  for (auto i = 0; i < numInput; ++i) {
    auto partition = partitions_[i];
    rawIndices[partition][maxIndex[partition]] = i;
    ++maxIndex[partition];
  }

  for (auto partition = 0; partition < numPartitions_; partition++) {
    auto partitionSize = maxIndex[partition];
    if (partitionSize == 0) {
      // Do not enqueue empty partitions.
      continue;
    }

    struct BufferReleaser {
      explicit BufferReleaser(BufferPtr& parent) : parent_(parent.get()) {}
      void addRef() const {
        parent_->addRef();
      }
      void release() const {
        parent_->release();
      }

     private:
      Buffer* parent_;
    };

    auto bufferView = BufferView<BufferReleaser>::create(
        reinterpret_cast<uint8_t*>(rawIndices[partition]),
        partitionSize * sizeof(vector_size_t),
        BufferReleaser(indexBuffer));
    auto partitionData =
        wrapChildren(input, partitionSize, std::move(bufferView));

    ContinueFuture future;
    auto reason = queues_[partition]->enqueue(partitionData, &future);
    if (FOLLY_UNLIKELY(reason == BlockingReason::kYield)) {
      // [morsel] when enqueue to any localExchangeQueue returns yield, it means
      // the LocalExchangeQueues have been aborted (possibly due to
      // skipProbeOnEmptyBuild, in this case, we need to issue noMoreInput to
      // halt the driver.

      // @zj: find the correct way to yield producing pipeline when consuming
      // pipeline is aborted in skipProbeOnEmptyBuild
      //      notifyPeersToTerminate();
      //      noMoreInput();
    } else if (reason != BlockingReason::kNotBlocked) {
      blockingReasons_.push_back(reason);
      futures_.push_back(std::move(future));
    }
  }
}

BlockingReason LocalPartition::isBlocked(ContinueFuture* future) {
  if (!futures_.empty()) {
    auto blockingReason = blockingReasons_.front();
    *future = folly::collectAll(futures_.begin(), futures_.end()).unit();
    futures_.clear();
    blockingReasons_.clear();
    return blockingReason;
  }

  return BlockingReason::kNotBlocked;
}

void LocalPartition::noMoreInput() {
  Operator::noMoreInput();
  for (const auto& queue : queues_) {
    queue->noMoreData();
  }
}

bool LocalPartition::isFinished() {
  if (!futures_.empty() || !noMoreInput_) {
    return false;
  }

  return true;
}
} // namespace bytedance::bolt::exec
