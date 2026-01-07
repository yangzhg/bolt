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

#include <cstdint>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/vector/ComplexVector.h"

#include <folly/Likely.h>
namespace bytedance::bolt {

/// Abstract class defining the interface for a stream of values to read in
/// batch by UnorderedStreamReader.
class BatchStream {
 public:
  virtual ~BatchStream() = default;

  /// Returns the next batch from the stream. The function returns true with
  /// read data in 'batch', otherwise returns false if it reaches to the end of
  /// the stream.
  virtual bool nextBatch(RowVectorPtr& batch) = 0;

  virtual uint32_t nextBatch(std::vector<char*>&) {
    BOLT_UNREACHABLE("Only called for row based spill")
  }

  virtual void reuse() {
    BOLT_UNREACHABLE("BatchStream::reuse should never be called")
  }

  virtual uint64_t getSpillReadIOTime() {
    return 0;
  };

  virtual void prefetch(){};
};

/// Implements a unordered reader to read from a group of streams with one at a
/// time in batch. The reader owns the streams. At each call of nextBatch(), it
/// returns the next batch from the current reading stream, and the reader will
/// switch to the next stream internally if the current stream reaches to the
/// end.
///
/// NOTE: this object is not thread safe.
template <typename BatchStream>
class UnorderedStreamReader {
 public:
  explicit UnorderedStreamReader(
      std::vector<std::unique_ptr<BatchStream>> streams)
      : currentStream(0), streams_(std::move(streams)) {
    static_assert(std::is_base_of_v<BatchStream, BatchStream>);
  }

  /// Returns the next batch from the current reading stream. The function will
  /// switch to the next stream if the current stream reaches to the end. The
  /// function returns true with read data in 'batch', otherwise returns
  /// false if all the streams have been read out.
  bool nextBatch(RowVectorPtr& batch) {
    MicrosecondTimer timer(&spillReadTimeUs_);
    while (FOLLY_LIKELY(currentStream < streams_.size())) {
      if (FOLLY_LIKELY(streams_[currentStream]->nextBatch(batch))) {
        return true;
      }
      spillReadIOTimeUs_ += streams_[currentStream]->getSpillReadIOTime();
      ++currentStream;
    }
    return false;
  }

  uint32_t nextBatch(std::vector<char*>& rows) {
    while (FOLLY_LIKELY(currentStream < streams_.size())) {
      auto size = streams_[currentStream]->nextBatch(rows);
      if (FOLLY_LIKELY(size)) {
        return size;
      }
      ++currentStream;
    }
    return 0;
  }

  uint64_t getSpillReadTime() const {
    return spillReadTimeUs_;
  }

  uint64_t getSpillDecompressTime() const {
    return 0;
  }

  void reuse() {
    currentStream = 0;
    for (auto& stream : streams_) {
      stream->reuse();
    }
  }

  uint64_t getSpillReadIOTime() const {
    return spillReadIOTimeUs_;
  }

 private:
  // Points to the current reading stream in 'streams_'.
  vector_size_t currentStream{0};
  // A list of streams to read in batch sequentially.
  std::vector<std::unique_ptr<BatchStream>> streams_;

  uint64_t spillReadTimeUs_{0};
  uint64_t spillReadIOTimeUs_{0};
};

} // namespace bytedance::bolt
