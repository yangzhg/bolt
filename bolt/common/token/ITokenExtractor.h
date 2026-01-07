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

#include "bolt/common/base/BloomFilter.h"
namespace bytedance::bolt {

/// Interface for string parsers.
struct ITokenExtractor {
  virtual ~ITokenExtractor() = default;
  virtual const char* getName() const = 0;

  /// Special implementation for creating bloom filter for LIKE function.
  /// It skips unescaped `%` and `_` and supports escaping symbols, but it is
  /// less lightweight.
  virtual bool nextInStringLike(
      const char* data,
      uint32_t length,
      uint32_t* __restrict pos,
      std::string& out) const = 0;
  //  virtual void stringLikeToBloomFilter(const char * data, uint32_t length,
  //  BloomFilter<std::string> & bloom_filter) const = 0;
  virtual void stringLikeToBloomFilter(
      const char* data,
      uint32_t length,
      NGramBloomFilter& bloom_filter) const = 0;
  static std::vector<std::string> splitToStringVector(
      const char* data,
      uint32_t length);
};

using TokenExtractorPtr = const ITokenExtractor*;

template <typename Derived>
class ITokenExtractorHelper : public ITokenExtractor {
  //  void stringLikeToBloomFilter(const char * data, uint32_t length,
  //  BloomFilter<std::string> & bloom_filter) const override
  void stringLikeToBloomFilter(
      const char* data,
      uint32_t length,
      NGramBloomFilter& bloom_filter) const override {
    uint32_t cur = 0;
    std::string token;

    while (cur < length &&
           static_cast<const Derived*>(this)->nextInStringLike(
               data, length, &cur, token))
      bloom_filter.insert(token);
  }
};

/// Parser extracting all ngrams from string.
struct NgramTokenExtractor final
    : public ITokenExtractorHelper<NgramTokenExtractor> {
  explicit NgramTokenExtractor(uint32_t ngram) : ngram_(ngram) {}
  const char* getName() const override {
    return "ngrambf";
  }
  bool nextInStringLike(
      const char* data,
      uint32_t length,
      uint32_t* __restrict pos,
      std::string& token) const override;
  uint32_t getN() const {
    return ngram_;
  }

 private:
  uint32_t ngram_;
};

/// Parser extracting tokens (sequences of numbers and ascii letters).
struct SplitTokenExtractor final
    : public ITokenExtractorHelper<SplitTokenExtractor> {
  const char* getName() const override {
    return "tokenbf";
  }
  bool nextInStringLike(
      const char* data,
      uint32_t length,
      uint32_t* __restrict pos,
      std::string& token) const override;
};

} // namespace bytedance::bolt
