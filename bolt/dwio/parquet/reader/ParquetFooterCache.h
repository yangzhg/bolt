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

#pragma once

#include <glog/logging.h>
#include <xxhash.h>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <folly/container/EvictingCacheMap.h>

#include "bolt/dwio/parquet/thrift/codegen/parquet_types.h"
namespace bytedance::bolt::parquet {

struct ParquetFileMetadata {
  size_t size;
  size_t time;
  std::shared_ptr<parquet::thrift::FileMetaData> metadata;
};

class ParquetFooterCache {
 private:
  class Impl {
   public:
    Impl(uint64_t max_size, size_t capture)
        : max_size_(max_size), lru_(capture, 1) {}

    bool add(
        const std::string& key,
        const std::shared_ptr<parquet::thrift::FileMetaData>& value,
        size_t size);

    std::optional<std::shared_ptr<parquet::thrift::FileMetaData>> get(
        const std::string& key);

    size_t applyTTL(size_t ttlSecs);

   private:
    const uint64_t max_size_;
    uint64_t size_ = 0;
    folly::EvictingCacheMap<std::string, ParquetFileMetadata> lru_;
    std::mutex cacheMu_;
  };

  explicit ParquetFooterCache(uint64_t max_size, size_t num_caches);

 public:
  static std::unique_ptr<ParquetFooterCache> instance_;

  static ParquetFooterCache* create(uint64_t max_size, size_t num_caches = 16) {
    if (instance_ == nullptr) {
      instance_ = std::unique_ptr<ParquetFooterCache>(
          new ParquetFooterCache(max_size, num_caches));
    }
    return instance_.get();
  }

  static ParquetFooterCache* getInstance() {
    if (instance_ != nullptr) {
      return instance_.get();
    }
    return nullptr;
  }

  inline bool add(
      const std::string& key,
      const std::shared_ptr<parquet::thrift::FileMetaData>& value,
      size_t size) {
    return caches_[get_shard(key)]->add(key, value, size);
  }

  inline std::optional<std::shared_ptr<parquet::thrift::FileMetaData>> get(
      const std::string& key) {
    return caches_[get_shard(key)]->get(key);
  }

  void applyTTL(size_t ttlSecs) {
    size_t total_clean = 0;
    for (const auto& cache : caches_) {
      total_clean += cache->applyTTL(ttlSecs);
    }
    if (total_clean > 0) {
      LOG(INFO) << "parquet footer cache applying cache TTL of " << ttlSecs
                << " seconds. Entries from " << total_clean
                << " objects are to be removed";
    }
  }

 private:
  static constexpr uint32_t kSeed = 0;

  const size_t num_caches_;
  const uint64_t max_size_;
  std::vector<std::unique_ptr<Impl>> caches_;

  inline int32_t get_shard(const std::string& key) {
    return XXH64(
               reinterpret_cast<const void*>(key.c_str()),
               key.length(),
               kSeed) %
        num_caches_;
  }
};
} // namespace bytedance::bolt::parquet
