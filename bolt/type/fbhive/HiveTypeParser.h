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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bolt/type/TypeParser.h"
#include "folly/Range.h"
namespace bytedance::bolt::type::fbhive {

// TODO : Find out what to do with these types
// NUMERIC, INTERVAL, VARCHAR, VOID
enum class TokenType {
  Boolean,
  Byte,
  Short,
  Integer,
  Date,
  Long,
  Float,
  Double,
  String,
  Binary,
  Timestamp,
  List,
  Map,
  Struct,
  StartSubType,
  EndSubType,
  Colon,
  Comma,
  Number,
  Identifier,
  EndOfStream,
  Decimal,
  LeftRoundBracket,
  RightRoundBracket,
  MaxTokenType
};

struct TokenMetadata {
  TokenType tokenType;
  bolt::TypeKind typeKind;
  std::vector<std::string> tokenString;
  bool isPrimitiveType;

  TokenMetadata(
      TokenType typ,
      bolt::TypeKind kind,
      std::vector<std::string>&& ts,
      bool ip)
      : tokenType(typ),
        typeKind(kind),
        tokenString(std::move(ts)),
        isPrimitiveType(ip) {}
};

struct Token {
  TokenMetadata* metadata;
  folly::StringPiece value;

  TokenType tokenType() const;

  bolt::TypeKind typeKind() const;

  bool isPrimitiveType() const;

  bool isValidType() const;

  bool isEOS() const;
};

struct TokenAndRemaining : public Token {
  folly::StringPiece remaining;
};

struct Result {
  std::shared_ptr<const bolt::Type> type;
};

struct ResultList {
  std::vector<std::shared_ptr<const bolt::Type>> typelist;
  std::vector<std::string> names;
};

class HiveTypeParser : public type::TypeParser {
 public:
  HiveTypeParser();

  ~HiveTypeParser() override = default;

  std::shared_ptr<const bolt::Type> parse(const std::string& ser) override;

 private:
  int8_t makeTokenId(TokenType tokenType) const;

  Result parseType();

  ResultList parseTypeList(bool hasFieldNames);

  TokenType lookAhead() const;

  Token eatToken(TokenType tokenType, bool ignorePredefined = false);

  Token nextToken(bool ignorePredefined = false);

  TokenAndRemaining nextToken(
      folly::StringPiece sp,
      bool ignorePredefined = false) const;

  TokenAndRemaining makeExtendedToken(
      TokenMetadata* tokenMetadata,
      folly::StringPiece sp,
      size_t len) const;

  template <TokenType KIND, bolt::TypeKind TYPEKIND>
  void setupMetadata(const char* tok = "") {
    setupMetadata<KIND, TYPEKIND>(std::vector<std::string>{std::string{tok}});
  }

  template <TokenType KIND, bolt::TypeKind TYPEKIND>
  void setupMetadata(std::vector<std::string>&& tokens) {
    static constexpr bool isPrimitive =
        bolt::TypeTraits<TYPEKIND>::isPrimitiveType;
    metadata_[makeTokenId(KIND)] = std::make_unique<TokenMetadata>(
        KIND, TYPEKIND, std::move(tokens), isPrimitive);
  }

  TokenMetadata* getMetadata(TokenType type) const;

 private:
  std::vector<std::unique_ptr<TokenMetadata>> metadata_;
  folly::StringPiece remaining_;
};

} // namespace bytedance::bolt::type::fbhive
