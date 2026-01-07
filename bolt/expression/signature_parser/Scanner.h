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

#include <cmath>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

#include "bolt/common/base/Exceptions.h"
#include "bolt/expression/TypeSignature.h"
namespace bytedance::bolt::exec {

class Scanner : public yyFlexLexer {
 public:
  Scanner(
      std::istream& arg_yyin,
      std::ostream& arg_yyout,
      TypeSignaturePtr& outputType,
      const std::string_view input)
      : yyFlexLexer(&arg_yyin, &arg_yyout),
        outputType_(outputType),
        input_(input){};
  int lex(Parser::semantic_type* yylval);

  void setTypeSignature(TypeSignaturePtr type) {
    outputType_ = std::move(type);
  }

  // Store input to print it as part of the error message.
  std::string_view input() {
    return input_;
  }

 private:
  TypeSignaturePtr& outputType_;
  const std::string_view input_;
};

} // namespace bytedance::bolt::exec
