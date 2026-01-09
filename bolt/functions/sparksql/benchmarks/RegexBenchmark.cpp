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

#include <iostream>
#include <random>
#include <string>
#include <vector>

#include <folly/Benchmark.h>
#include <folly/Synchronized.h>
#include <folly/container/EvictingCacheMap.h>
#include <folly/init/Init.h>
#include <re2/re2.h>
#include <unicode/regex.h>

constexpr int kMaxRegexSize = 100;
enum REGEX_TYPE { ICU, RE2 };
static std::vector<std::string> testStrings;
static std::vector<std::string> patterns10{
    "\\b\\w+\\b",
    "[0-9]+",
    "[a-z]+",
    ".*",
    "\\d{3}-\\d{2}-\\d{4}",
    "^\\w+@[a-zA-Z_]+?\\.[a-zA-Z]{2,3}$",
    "^https?:\\/\\/[^\\s]+$",
    "\\b(?:0[xX])?[0-9a-fA-F]+\\b",
    "\\btrue\\b|\\bfalse\\b",
    "\\d{1,2}\\/\\d{1,2}\\/\\d{2,4}"};
static std::vector<std::string> patterns20{
    "\\b\\w+\\b",
    "[0-9]+",
    "[a-z]+",
    ".*",
    "\\d{3}-\\d{2}-\\d{4}",
    "^\\w+@[a-zA-Z_]+?\\.[a-zA-Z]{2,3}$",
    "^https?:\\/\\/[^\\s]+$",
    "\\b(?:0[xX])?[0-9a-fA-F]+\\b",
    "\\d{1,2}\\/\\d{1,2}\\/\\d{2,4}",
    "\\d{1,2}\\/\\d{1,2}\\/\\d{2,4}"};
static std::vector<std::string> patterns50{
    "[0-9]+",
    "[0-9]+",
    ".*",
    ".*",
    "^\\w+@[a-zA-Z_]+?\\.[a-zA-Z]{2,3}$",
    "^\\w+@[a-zA-Z_]+?\\.[a-zA-Z]{2,3}$",
    "\\b(?:0[xX])?[0-9a-fA-F]+\\b",
    "\\b(?:0[xX])?[0-9a-fA-F]+\\b",
    "\\d{1,2}\\/\\d{1,2}\\/\\d{2,4}",
    "\\d{1,2}\\/\\d{1,2}\\/\\d{2,4}"};
static std::vector<std::string> patterns90{
    "[0-9]+",
    "\\d{1,2}\\/\\d{1,2}\\/\\d{2,4}",
    "\\d{1,2}\\/\\d{1,2}\\/\\d{2,4}",
    "\\d{1,2}\\/\\d{1,2}\\/\\d{2,4}",
    "\\d{1,2}\\/\\d{1,2}\\/\\d{2,4}",
    "\\d{1,2}\\/\\d{1,2}\\/\\d{2,4}",
    "\\d{1,2}\\/\\d{1,2}\\/\\d{2,4}",
    "\\d{1,2}\\/\\d{1,2}\\/\\d{2,4}",
    "\\d{1,2}\\/\\d{1,2}\\/\\d{2,4}",
    "\\d{1,2}\\/\\d{1,2}\\/\\d{2,4}"};
// regex type: use icu or re2 regex engine
// direct: true: compile regex pattern directly and then match, false: find in
// cache (if not found, compile and insert) and then match thread safe: use
// thread safe cache or not, cache use folly::EvictingCacheMap
template <REGEX_TYPE type, bool Direct>
void testRegexPerformance(
    const std::vector<std::string>& testStrings,
    const std::vector<std::string>& patterns) {
  if constexpr (type == REGEX_TYPE::ICU) {
    if constexpr (Direct) {
      // compile and use ICU regex pattern directly
      for (const std::string& pattern : patterns) {
        auto up = icu::UnicodeString::fromUTF8(pattern);
        UErrorCode status = U_ZERO_ERROR;
        auto regexPattern = icu::RegexPattern::compile(up, up.length(), status);
        for (const std::string& text : testStrings) {
          auto matcher =
              regexPattern->matcher(icu::UnicodeString::fromUTF8(text), status);
          bool match = matcher->find();
        }
      }
    } else {
      folly::EvictingCacheMap<std::string, std::unique_ptr<icu::RegexPattern>>
          cache{kMaxRegexSize};
      folly::Synchronized<folly::EvictingCacheMap<
          std::string,
          std::unique_ptr<icu::RegexPattern>>>
          syncCache{folly::EvictingCacheMap<
              std::string,
              std::unique_ptr<icu::RegexPattern>>(kMaxRegexSize)};
      UErrorCode status = U_ZERO_ERROR;
      for (const std::string& pattern : patterns) {
        for (const std::string& text : testStrings) {
          auto it = cache.find(pattern);
          if (it == cache.end()) {
            auto up = icu::UnicodeString::fromUTF8(pattern);
            auto p = icu::RegexPattern::compile(up, up.length(), status);
            std::unique_ptr<icu::RegexPattern> ptr(p);
            auto result = cache.insert(pattern, std::move(ptr));
            it = result.first;
          }
          auto matcher =
              it->second->matcher(icu::UnicodeString::fromUTF8(text), status);
          bool match = matcher->find();
        }
      }
    }
  } else if constexpr (type == REGEX_TYPE::RE2) {
    if constexpr (Direct) {
      // compile and use RE2 regex pattern directly
      for (const std::string& pattern : patterns) {
        re2::RE2::Options opt{re2::RE2::Quiet};
        opt.set_dot_nl(true);
        re2::RE2 regex(pattern, opt);
        for (const std::string& text : testStrings) {
          re2::RE2::FullMatch(text, regex);
        }
      }
    } else {
      // compile and use RE2 regex pattern from cache
      folly::EvictingCacheMap<std::string, std::unique_ptr<re2::RE2>> cache{
          kMaxRegexSize};
      folly::Synchronized<
          folly::EvictingCacheMap<std::string, std::unique_ptr<re2::RE2>>>
          syncCache{
              folly::EvictingCacheMap<std::string, std::unique_ptr<re2::RE2>>(
                  kMaxRegexSize)};
      for (const std::string& pattern : patterns) {
        for (const std::string& text : testStrings) {
          re2::RE2::Options opt{re2::RE2::Quiet};
          opt.set_dot_nl(true);
          auto it = cache.find(pattern);
          if (it == cache.end()) {
            re2::RE2::Options opt{re2::RE2::Quiet};
            opt.set_dot_nl(true);
            auto result =
                cache.insert(pattern, std::make_unique<re2::RE2>(pattern, opt));
            it = result.first;
            re2::RE2::FullMatch(text, *it->second);
          }
        }
      }
    }
  }
}

std::string generate_random_string(size_t length) {
  const std::string chars =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  std::random_device rd;
  std::mt19937 generator(rd());
  std::uniform_int_distribution<> distribution(0, chars.size() - 1);

  std::string result;
  for (size_t i = 0; i < length; i++) {
    result += chars[distribution(generator)];
  }
  return result;
}

std::vector<std::string> fill_test_strings(size_t num_strings, size_t length) {
  std::vector<std::string> strings;
  for (size_t i = 0; i < num_strings; i++) {
    strings.push_back(generate_random_string(length));
  }
  return strings;
}

template <REGEX_TYPE type>
void testRegexCompile(const std::vector<std::string>& patterns, int round = 1) {
  for (int i = 0; i < round; i++) {
    for (const std::string& pattern : patterns) {
      if constexpr (type == REGEX_TYPE::ICU) {
        UErrorCode status = U_ZERO_ERROR;
        auto regexPattern =
            icu::RegexPattern::compile(pattern.c_str(), pattern.size(), status);
        if (status != U_ZERO_ERROR) {
          std::cout << "Error compiling pattern: " << pattern << std::endl;
        }
      } else if constexpr (type == REGEX_TYPE::RE2) {
        re2::RE2::Options opt{re2::RE2::Quiet};
        opt.set_dot_nl(true);
        re2::RE2 regex(pattern, opt);
        if (!regex.ok()) {
          std::cout << "Error compiling pattern: " << pattern << std::endl;
        }
      }
    }
  }
}

BENCHMARK(ICU_Direct_10) {
  testRegexPerformance<REGEX_TYPE::ICU, true>(testStrings, patterns10);
}

BENCHMARK(ICU_Direct_20) {
  testRegexPerformance<REGEX_TYPE::ICU, true>(testStrings, patterns20);
}

BENCHMARK(ICU_Direct_50) {
  testRegexPerformance<REGEX_TYPE::ICU, true>(testStrings, patterns50);
}

BENCHMARK(ICU_Direct_90) {
  testRegexPerformance<REGEX_TYPE::ICU, true>(testStrings, patterns90);
}

BENCHMARK(ICU_Cache_10) {
  testRegexPerformance<REGEX_TYPE::ICU, false>(testStrings, patterns10);
}

BENCHMARK(ICU_Cache_20) {
  testRegexPerformance<REGEX_TYPE::ICU, false>(testStrings, patterns20);
}

BENCHMARK(ICU_Cache_50) {
  testRegexPerformance<REGEX_TYPE::ICU, false>(testStrings, patterns50);
}

BENCHMARK(ICU_Cache_90) {
  testRegexPerformance<REGEX_TYPE::ICU, false>(testStrings, patterns90);
}

BENCHMARK(RE2_Direct_10) {
  testRegexPerformance<REGEX_TYPE::RE2, true>(testStrings, patterns10);
}

BENCHMARK(RE2_Direct_20) {
  testRegexPerformance<REGEX_TYPE::RE2, true>(testStrings, patterns20);
}

BENCHMARK(RE2_Direct_50) {
  testRegexPerformance<REGEX_TYPE::RE2, true>(testStrings, patterns50);
}

BENCHMARK(RE2_Direct_90) {
  testRegexPerformance<REGEX_TYPE::RE2, true>(testStrings, patterns90);
}

BENCHMARK(RE2_Cache_10) {
  testRegexPerformance<REGEX_TYPE::RE2, false>(testStrings, patterns10);
}

BENCHMARK(RE2_Cache_20) {
  testRegexPerformance<REGEX_TYPE::RE2, false>(testStrings, patterns20);
}

BENCHMARK(RE2_Cache_50) {
  testRegexPerformance<REGEX_TYPE::RE2, false>(testStrings, patterns50);
}

BENCHMARK(RE2_Cache_90) {
  testRegexPerformance<REGEX_TYPE::RE2, false>(testStrings, patterns90);
}

BENCHMARK(ICU_Compile) {
  testRegexCompile<REGEX_TYPE::ICU>(patterns10);
}
BENCHMARK(RE2_Compile) {
  testRegexCompile<REGEX_TYPE::RE2>(patterns10);
}

int main(int argc, char** argv) {
  testStrings = fill_test_strings(1000, 20);
  folly::runBenchmarks();
  return 0;
}
