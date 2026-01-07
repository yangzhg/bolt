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

#include "bolt/dwio/dwrf/reader/StripeDictionaryCache.h"
namespace bytedance::bolt::dwrf {
StripeDictionaryCache::DictionaryEntry::DictionaryEntry(
    folly::Function<BufferPtr(bolt::memory::MemoryPool*)>&& dictGen)
    : dictGen_{std::move(dictGen)} {}

BufferPtr StripeDictionaryCache::DictionaryEntry::getDictionaryBuffer(
    bolt::memory::MemoryPool* pool) {
  folly::call_once(onceFlag_, [&]() {
    dictionaryBuffer_ = dictGen_(pool);
    dictGen_ = nullptr;
  });
  return dictionaryBuffer_;
}

StripeDictionaryCache::StripeDictionaryCache(bolt::memory::MemoryPool* pool)
    : pool_{pool} {}

// It might be more elegant to pass in a StripeStream here instead.
void StripeDictionaryCache::registerIntDictionary(
    const EncodingKey& ek,
    folly::Function<BufferPtr(bolt::memory::MemoryPool*)>&& dictGen) {
  intDictionaryFactories_.emplace(
      ek, std::make_unique<DictionaryEntry>(std::move(dictGen)));
}

BufferPtr StripeDictionaryCache::getIntDictionary(const EncodingKey& ek) {
  return intDictionaryFactories_.at(ek)->getDictionaryBuffer(pool_);
}

} // namespace bytedance::bolt::dwrf
