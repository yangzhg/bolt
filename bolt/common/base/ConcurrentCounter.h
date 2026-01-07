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

#include <stddef.h>
#include <stdint.h>
#include <new>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
namespace bytedance::bolt {

/// The class provides concurrent updates to a counter with minimum lock
/// contention. The template argument T specifies the counter type. The counter
/// is N-way sharded internally. Each update goes to one of the sharded counters
/// based on the update thread id.
template <class T>
class ConcurrentCounter {
 public:
  /// Creates a concurrent counter with specified number of shards.
  ///
  /// NOTE: the constructor sets the actual number of shards to be the next
  /// power of 2.
  explicit ConcurrentCounter(size_t numShards)
      : numShards_(bits::nextPowerOfTwo(numShards)),
        shardMask_(numShards_ - 1),
        counters_(numShards_) {
    BOLT_CHECK_GE(numShards_, 1);
    for (auto& counter : counters_) {
      counter.value = T();
    }
  }

  ConcurrentCounter(const ConcurrentCounter&) = delete;
  ConcurrentCounter& operator=(const ConcurrentCounter&) = delete;

  /// Invoked to read the sum of values from 'counters_'.
  T read() const {
    T sum = T();
    for (size_t i = 0; i < numShards_; ++i) {
      sum += counters_[i].read();
    }
    return sum;
  }

  /// Invoked to update with 'delta'.
  void update(T delta) {
    counters_[shardIndex()].update(delta);
  }

  /// Invoked to update with 'delta' and user provided 'updateFn'. The function
  /// picks up the shard to apply the customized update.
  using UpdateFn = std::function<bool(T& counter, T delta, std::mutex& lock)>;
  bool update(T delta, const UpdateFn& updateFn) {
    return counters_[shardIndex()].update(delta, updateFn);
  }

  void testingClear() {
    for (auto& counter : counters_) {
      counter.value = T();
    }
  }

  T testingRead(size_t index) const {
    return counters_[index].read();
  }

  bool testingUpdate(size_t index, T delta, const UpdateFn& updateFn) {
    return counters_[index].update(delta, updateFn);
  }

 private:
  struct alignas(folly::hardware_destructive_interference_size) Counter {
    mutable std::mutex lock;
    T value;

    T read() const {
      std::lock_guard<std::mutex> l(lock);
      return value;
    }

    void update(T delta) {
      std::lock_guard<std::mutex> l(lock);
      value += delta;
    }

    bool update(T delta, const UpdateFn& updateFn) {
      return updateFn(value, delta, lock);
    }
  };

  size_t shardIndex() const {
    const size_t hash =
        std::hash<std::thread::id>{}(std::this_thread::get_id());
    const size_t index = hash & shardMask_;
    BOLT_DCHECK_LT(index, counters_.size());
    return index;
  }

  const size_t numShards_;
  const size_t shardMask_;

  std::vector<Counter> counters_;
};
} // namespace bytedance::bolt
