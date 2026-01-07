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

#include <folly/Unit.h>
#include <folly/futures/Future.h>
namespace bytedance::bolt {
/// Simple wrapper around folly's promise to track down destruction of
/// unfulfilled promises.
template <class T>
class BoltPromise : public folly::Promise<T> {
 public:
  BoltPromise() : folly::Promise<T>() {}

  explicit BoltPromise(const std::string& context)
      : folly::Promise<T>(), context_(context) {}

  BoltPromise(
      folly::futures::detail::EmptyConstruct,
      const std::string& context) noexcept
      : folly::Promise<T>(folly::Promise<T>::makeEmpty()), context_(context) {}

  ~BoltPromise() {
    if (!this->isFulfilled()) {
      LOG(WARNING) << "PROMISE: Unfulfilled promise is being deleted. Context: "
                   << context_;
    }
  }

  explicit BoltPromise(BoltPromise<T>&& other)
      : folly::Promise<T>(std::move(other)),
        context_(std::move(other.context_)) {}

  BoltPromise& operator=(BoltPromise<T>&& other) noexcept {
    folly::Promise<T>::operator=(std::move(other));
    context_ = std::move(other.context_);
    return *this;
  }

  static BoltPromise makeEmpty(const std::string& context = "") noexcept {
    return BoltPromise<T>(folly::futures::detail::EmptyConstruct{}, context);
  }

 private:
  /// Optional parameter to understand where this promise was created.
  std::string context_;
};

using ContinuePromise = BoltPromise<folly::Unit>;
using ContinueFuture = folly::SemiFuture<folly::Unit>;

/// Equivalent of folly's makePromiseContract for BoltPromise.
static inline std::pair<ContinuePromise, ContinueFuture>
makeBoltContinuePromiseContract(const std::string& promiseContext = "") {
  auto p = ContinuePromise(promiseContext);
  auto f = p.getSemiFuture();
  return std::make_pair(std::move(p), std::move(f));
}

} // namespace bytedance::bolt
