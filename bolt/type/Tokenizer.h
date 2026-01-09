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

#include "bolt/type/Subfield.h"
namespace bytedance::bolt::common {

/**
 * Base tokenizer interface for parsing field paths.
 */
class Tokenizer {
 public:
  enum class State {
    // We have computed the next element and haven't returned it yet.
    kReady,

    // We haven't yet computed or have already returned the element.
    kNotReady,

    // We have reached the end of the data and are finished.
    kDone,

    // We've suffered an exception and are kaput.
    kFailed,
  };

  virtual ~Tokenizer() = default;

  /**
   * Returns true if there are more tokens available.
   */
  virtual bool hasNext() = 0;

  /**
   * Returns the next token in the sequence.
   * Throws if no tokens are available.
   */
  virtual std::unique_ptr<Subfield::PathElement> next() = 0;

  /**
   * Factory method to create a tokenizer instance for the given path.
   */
  static std::unique_ptr<Tokenizer> getInstance(const std::string& path);

  /**
   * Registers a custom tokenizer factory function.
   */
  static void registerInstanceFactory(
      std::function<std::unique_ptr<Tokenizer>(const std::string&)>
          tokenizerFactory);

 private:
  static std::function<std::unique_ptr<Tokenizer>(const std::string&)>
      tokenizerFactory_;
};

/**
 * Default implementation of the Tokenizer interface that handles UTF-8 encoded
 * paths.
 */
class DefaultTokenizer : public Tokenizer {
 public:
  /**
   * Creates a new tokenizer for the given path.
   */
  explicit DefaultTokenizer(const std::string& path);

  /**
   * Returns true if there are more tokens available.
   */
  bool hasNext() override;

  /**
   * Returns the next token in the sequence.
   * Throws if no tokens are available.
   */
  std::unique_ptr<Subfield::PathElement> next() override;

 private:
  // Structure to hold code point information during UTF-8 decoding
  struct CodePointInfo {
    char32_t codePoint;
    size_t length;
  };

  const std::string path_;
  // Customized separators to tokenize field name.
  std::shared_ptr<Separators> separators_;

  int index_;
  State state;
  bool firstSegment = true;
  std::unique_ptr<Subfield::PathElement> next_;

  // UTF-8 code point handling
  CodePointInfo decodeCurrentCodePoint();
  char32_t peekCodePoint();
  void consumeCodePoint();
  void consumeCurrentCodePoint();

  // Character classification
  bool isUnquotedPathCharacter(char32_t cp);
  bool isUnquotedSubscriptCharacter(char32_t cp);

  // Token matching
  bool hasNextCharacter();
  bool tryMatchSeparator(char32_t expected);
  void match(char32_t expected);
  bool tryMatch(char32_t expected);

  // Path element matching
  std::unique_ptr<Subfield::PathElement> computeNext();
  std::unique_ptr<Subfield::PathElement> matchPathSegment();
  std::unique_ptr<Subfield::PathElement> matchUnquotedSubscript();
  std::unique_ptr<Subfield::PathElement> matchQuotedSubscript();
  std::unique_ptr<Subfield::PathElement> matchWildcardSubscript();

  // Error handling
  void invalidSubfieldPath();
  std::string toString();
  bool tryToComputeNext();
};

} // namespace bytedance::bolt::common
