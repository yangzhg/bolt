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

#include "bolt/common/base/BloomFilter.h"
#include <city.h>
#include <cmath>
#include <cstdlib>
#include "bolt/type/Type.h"
namespace bytedance::bolt {
constexpr uint32_t BlockSplitBloomFilter::SALT[kBitsSetPerBlock];

std::string MetaBloomFilter::toString() const {
  std::string strKind;
  switch (kind_) {
    case BloomFilterKind::kParquet:
      strKind = "BlockSplit";
      break;
    case BloomFilterKind::kNGram:
      strKind = "NGram";
      break;
    default:
      break;
  }
  return fmt::format("BloomFilter({})", strKind);
}

NGramBloomFilter::NGramBloomFilter(int64_t size, int32_t hashes, int32_t seed)
    : MetaBloomFilter(BloomFilterKind::kNGram),
      size_(size),
      hashes_(hashes),
      seed_(seed),
      words_((size_ + sizeof(uint64_t) - 1) / sizeof(uint64_t)),
      filter_(words_, 0) {
  BOLT_CHECK(size_ != 0);
  BOLT_CHECK(hashes_ != 0);
}

NGramBloomFilter::NGramBloomFilter(
    int64_t size,
    int32_t hashes,
    int32_t seed,
    const std::vector<uint64_t>& bits)
    : MetaBloomFilter(BloomFilterKind::kNGram),
      size_(size),
      hashes_(hashes),
      seed_(seed),
      words_((size_ + sizeof(uint64_t) - 1) / sizeof(uint64_t)) {
  BOLT_CHECK(size_ != 0);
  BOLT_CHECK(hashes_ != 0);
  BOLT_CHECK(
      bits.size() == words_, "bits size = {}, words = {}", bits.size(), words_);
  filter_ = bits;
}

NGramBloomFilter::NGramBloomFilter(
    int64_t size,
    int32_t hashes,
    int32_t seed,
    const char* bits)
    : MetaBloomFilter(BloomFilterKind::kNGram),
      size_(size),
      hashes_(hashes),
      seed_(seed),
      words_((size_ + sizeof(uint64_t) - 1) / sizeof(uint64_t)) {
  BOLT_CHECK(size_ != 0);
  BOLT_CHECK(hashes_ != 0);
  filter_.reserve(size_);
  memcpy(filter_.data(), bits, size_);
}

bool NGramBloomFilter::mayContain(const char* data, size_t len) const {
  int64_t hash1 = CityHash64WithSeed(data, len, seed_);
  int64_t hash2 =
      CityHash64WithSeed(data, len, SEED_GEN_A * seed_ + SEED_GEN_B);

  for (int64_t i = 0; i < hashes_; ++i) {
    int64_t pos = (hash1 + i * hash2 + i * i) % (8 * size_);
    pos = std::abs(pos);
    if (!(filter_[pos / (8 * sizeof(uint64_t))] &
          (1ULL << (pos % (8 * sizeof(uint64_t))))))
      return false;
  }
  return true;
}

void NGramBloomFilter::insert(const char* data, size_t len) {
  int64_t hash1 = CityHash64WithSeed(data, len, seed_);
  int64_t hash2 =
      CityHash64WithSeed(data, len, SEED_GEN_A * seed_ + SEED_GEN_B);

  for (int64_t i = 0; i < hashes_; ++i) {
    int64_t pos = (hash1 + i * hash2 + i * i) % (8 * size_);
    pos = std::abs(pos);
    filter_[pos / (8 * sizeof(uint64_t))] |=
        (1ULL << (pos % (8 * sizeof(uint64_t))));
  }
}

void NGramBloomFilter::clear() {
  filter_.assign(words_, 0);
  BOLT_CHECK_EQ(filter_[0], 0);
}

bool NGramBloomFilter::contains(const NGramBloomFilter& bf) {
  for (int i = 0; i < words_; ++i) {
    if ((filter_[i] & bf.filter_[i]) != bf.filter_[i])
      return false;
  }
  return true;
}

bool NGramBloomFilter::isEmpty() const {
  for (int i = 0; i < words_; ++i)
    if (filter_[i] != 0)
      return false;
  return true;
}

BlockSplitBloomFilter::BlockSplitBloomFilter(int32_t num_bytes)
    : MetaBloomFilter(BloomFilterKind::kParquet), num_bytes_(num_bytes) {
  BOLT_CHECK_LE(num_bytes_, kMaximumBloomFilterBytes);
  if (num_bytes_ < kMinimumBloomFilterBytes) {
    num_bytes_ = kMinimumBloomFilterBytes;
  }
  data_.resize(num_bytes_, 0);
}

BlockSplitBloomFilter::BlockSplitBloomFilter(
    const uint8_t* bitset,
    int32_t num_bytes)
    : MetaBloomFilter(BloomFilterKind::kParquet), num_bytes_(num_bytes) {
  BOLT_CHECK_LE(num_bytes_, kMaximumBloomFilterBytes);
  if (num_bytes_ < kMinimumBloomFilterBytes) {
    num_bytes_ = kMinimumBloomFilterBytes;
  }
  data_.resize(num_bytes_);
  memcpy(data_.data(), bitset, num_bytes);
}

void BlockSplitBloomFilter::SetMask(uint32_t key, BlockMask& block_mask) const {
  for (int i = 0; i < kBitsSetPerBlock; ++i) {
    block_mask.item[i] = key * SALT[i];
  }

  for (int i = 0; i < kBitsSetPerBlock; ++i) {
    block_mask.item[i] = block_mask.item[i] >> 27;
  }

  for (int i = 0; i < kBitsSetPerBlock; ++i) {
    block_mask.item[i] = UINT32_C(0x1) << block_mask.item[i];
  }
}

bool BlockSplitBloomFilter::mayContain(uint64_t hash) const {
  const uint32_t bucket_index = static_cast<uint32_t>(
      ((hash >> 32) * (num_bytes_ / kBytesPerFilterBlock)) >> 32);
  const uint32_t key = static_cast<uint32_t>(hash);
  const uint32_t* bitset32 = reinterpret_cast<const uint32_t*>(data_.data());

  // Calculate mask for bucket.
  BlockMask block_mask;
  SetMask(key, block_mask);

  for (int i = 0; i < kBitsSetPerBlock; ++i) {
    if (0 ==
        (bitset32[kBitsSetPerBlock * bucket_index + i] & block_mask.item[i])) {
      return false;
    }
  }
  return true;
}

void BlockSplitBloomFilter::insert(uint64_t hash) {
  const uint32_t bucket_index = static_cast<uint32_t>(
      ((hash >> 32) * (num_bytes_ / kBytesPerFilterBlock)) >> 32);
  const uint32_t key = static_cast<uint32_t>(hash);
  uint32_t* bitset32 = reinterpret_cast<uint32_t*>(data_.data());

  // Calculate mask for bucket.
  BlockMask block_mask;
  SetMask(key, block_mask);

  for (int i = 0; i < kBitsSetPerBlock; i++) {
    bitset32[bucket_index * kBitsSetPerBlock + i] |= block_mask.item[i];
  }
}

} // namespace bytedance::bolt
