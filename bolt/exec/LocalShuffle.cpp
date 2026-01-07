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

#include "bolt/exec/LocalShuffle.h"

#include <algorithm>

#include <exec/OperatorMetric.h>
#include "bolt/exec/OperatorUtils.h"
#include "bolt/exec/Task.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::exec {
LocalShuffle::LocalShuffle(
    int32_t operatorId,
    DriverCtx* FOLLY_NONNULL driverCtx,
    const std::shared_ptr<const core::LocalShuffleNode>& localShuffleNode)
    : Operator(
          driverCtx,
          localShuffleNode->outputType(),
          operatorId,
          localShuffleNode->id(),
          "LocalShuffle",
          localShuffleNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(
                    operatorId,
                    driverCtx->queryConfig().rowBasedSpillMode())
              : std::nullopt),
      seed_(localShuffleNode->seed()),
      poolSize_(driverCtx->queryConfig().localShufflePoolSize()),
      gen_(seed_) {
  maxOutputRows_ = outputBatchRows(std::nullopt);

  data_ = std::make_unique<RowContainer>(outputType_->children(), pool());
}

void LocalShuffle::addInput(RowVectorPtr input) {
  BOLT_CHECK(!noMoreInput_);
  auto size = input->size();
  data_->store(input);
  numInputRows_ += size;
  currentRows_ += size;
  if (currentRows_ >= poolSize_) {
    needsInput_ = false;
    shuffle();
  }
}
void LocalShuffle::shuffle() {
  shuffledRows_.resize(currentRows_);
  RowContainerIterator iter;
  data_->listRows(&iter, currentRows_, shuffledRows_.data());
  std::shuffle(shuffledRows_.begin(), shuffledRows_.end(), gen_);
  shuffled_ = true;
  pool()->release();
}

void LocalShuffle::prepareOutput() {
  BOLT_CHECK_GT(maxOutputRows_, 0);
  const size_t batchSize = std::min<size_t>(currentRows_, maxOutputRows_);

  if (output_ != nullptr) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, batchSize);
    output_ = std::static_pointer_cast<RowVector>(output);
  } else {
    output_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(outputType_, batchSize, pool()));
  }

  for (auto& child : output_->children()) {
    child->resize(batchSize);
  }

  BOLT_CHECK_GT(output_->size(), 0);
  BOLT_DCHECK_LE(output_->size(), maxOutputRows_);
}

void LocalShuffle::noMoreInput() {
  Operator::noMoreInput();
  if (currentRows_ > 0 && !shuffled_) {
    shuffle();
  }
  noMoreInput_ = true;
}

RowVectorPtr LocalShuffle::getOutput() {
  if (!shuffled_) {
    return nullptr;
  }
  prepareOutput();
  for (auto i = 0; i < output_->childrenSize(); ++i) {
    data_->extractColumn(
        shuffledRows_.data() + numCurrentOutputRows_,
        output_->size(),
        i,
        output_->childAt(i));
  }
  numCurrentOutputRows_ += output_->size();
  currentRows_ -= output_->size();
  if (currentRows_ == 0) {
    numCurrentOutputRows_ = 0;
    needsInput_ = true;
    shuffled_ = false;
  }
  return output_;
}

bool LocalShuffle::isFinished() {
  return noMoreInput_;
}

bool LocalShuffle::needsInput() const {
  return needsInput_;
}

void LocalShuffle::close() {
  Operator::close();
  data_.reset();
  shuffledRows_.clear();
}
} // namespace bytedance::bolt::exec
