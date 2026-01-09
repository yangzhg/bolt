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

#include "bolt/dwio/parquet/reader/ParquetFooterCache.h"
#include "bolt/common/time/Timer.h"
namespace bytedance::bolt::parquet {
std::unique_ptr<ParquetFooterCache> ParquetFooterCache::instance_ = nullptr;

bool ParquetFooterCache::Impl::add(
    const std::string& key,
    const std::shared_ptr<parquet::thrift::FileMetaData>& value,
    size_t size) {
  auto current_time = getCurrentTimeSec();
  std::lock_guard<std::mutex> cache_lock(cacheMu_);
  while (size + size_ > max_size_ && lru_.size() > 0) {
    auto last = --lru_.end();
    size_ -= last->second.size;
    lru_.erase(last);
  }
  size_ += size;
  return lru_.insert(key, {size, current_time, value}).second;
}

std::optional<std::shared_ptr<parquet::thrift::FileMetaData>>
ParquetFooterCache::Impl::get(const std::string& key) {
  auto current_time = getCurrentTimeSec();
  std::lock_guard<std::mutex> cache_lock(cacheMu_);
  auto it = lru_.find(key);
  if (it == lru_.end()) {
    return std::nullopt;
  }
  it->second.time = current_time;
  return it->second.metadata;
}

size_t ParquetFooterCache::Impl::applyTTL(size_t ttlSecs) {
  auto maxOpenTime = getCurrentTimeSec() - ttlSecs;
  std::lock_guard<std::mutex> cache_lock(cacheMu_);
  size_t clean_num = 0;
  while (lru_.size() > 0) {
    auto last = --lru_.end();
    if (last->second.time < maxOpenTime) {
      size_ -= last->second.size;
      lru_.erase(last);
      clean_num++;
    } else {
      break;
    }
  }
  return clean_num;
}

ParquetFooterCache::ParquetFooterCache(uint64_t max_size, size_t num_caches)
    : num_caches_(num_caches), max_size_(max_size) {
  for (int i = 0; i < num_caches_; ++i) {
    auto impl =
        std::make_unique<Impl>(max_size / num_caches_, 200000 / num_caches_);
    caches_.push_back(std::move(impl));
  }
}

} // namespace bytedance::bolt::parquet
