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

#include <folly/Function.h>

#include "bolt/common/base/GTestMacros.h"
#include "bolt/dwio/common/IntDecoder.h"
#include "bolt/dwio/dwrf/common/Common.h"
#include "bolt/vector/BaseVector.h"
#include "folly/synchronization/CallOnce.h"
namespace bytedance::bolt::dwrf {
class StripeDictionaryCache {
  // This could be potentially made an interface to be shared for
  // string dictionaries. However, we will need a union return type
  // in that case.
  class DictionaryEntry {
   public:
    explicit DictionaryEntry(
        folly::Function<BufferPtr(bolt::memory::MemoryPool*)>&& dictGen);

    BufferPtr getDictionaryBuffer(bolt::memory::MemoryPool* pool);

   private:
    folly::Function<BufferPtr(bolt::memory::MemoryPool*)> dictGen_;
    BufferPtr dictionaryBuffer_;
    folly::once_flag onceFlag_;
  };

 public:
  explicit StripeDictionaryCache(bolt::memory::MemoryPool* pool);

  void registerIntDictionary(
      const EncodingKey& ek,
      folly::Function<BufferPtr(bolt::memory::MemoryPool*)>&& dictGen);

  BufferPtr getIntDictionary(const EncodingKey& ek);

 private:
  // This is typically the reader's memory pool.
  memory::MemoryPool* pool_;
  std::unordered_map<
      EncodingKey,
      std::unique_ptr<DictionaryEntry>,
      EncodingKeyHash>
      intDictionaryFactories_;

  BOLT_FRIEND_TEST(StripeDictionaryCacheTest, RegisterDictionary);
};

} // namespace bytedance::bolt::dwrf
