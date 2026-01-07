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

#include "bolt/expression/fuzzer/ArgumentTypeFuzzer.h"

#include <boost/algorithm/string.hpp>
#include <boost/random/uniform_int_distribution.hpp>

#include "bolt/expression/ReverseSignatureBinder.h"
#include "bolt/expression/SignatureBinder.h"
#include "bolt/type/Type.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
namespace bytedance::bolt::fuzzer {

std::string typeToBaseName(const TypePtr& type) {
  return boost::algorithm::to_lower_copy(std::string{type->kindName()});
}

std::optional<TypeKind> baseNameToTypeKind(const std::string& typeName) {
  auto kindName = boost::algorithm::to_upper_copy(typeName);
  return tryMapNameToTypeKind(kindName);
}

void ArgumentTypeFuzzer::determineUnboundedTypeVariables() {
  for (auto& [variableName, variableInfo] : variables()) {
    if (!variableInfo.isTypeParameter()) {
      continue;
    }

    if (bindings_[variableName] != nullptr) {
      continue;
    }

    // Random randomType() never generates unknown here.
    // TODO: we should extend randomType types and exclude unknown based
    // on variableInfo.
    if (variableInfo.orderableTypesOnly()) {
      bindings_[variableName] = randOrderableType();
    } else {
      bindings_[variableName] = randType();
    }
  }
}

TypePtr ArgumentTypeFuzzer::randType() {
  return bolt::randType(rng_, 2);
}

TypePtr ArgumentTypeFuzzer::randOrderableType() {
  return bolt::randOrderableType(rng_, 2);
}

bool ArgumentTypeFuzzer::fuzzArgumentTypes(uint32_t maxVariadicArgs) {
  const auto& formalArgs = signature_.argumentTypes();
  auto formalArgsCnt = formalArgs.size();

  if (returnType_) {
    exec::ReverseSignatureBinder binder{signature_, returnType_};
    if (!binder.tryBind()) {
      return false;
    }
    bindings_ = binder.bindings();
  } else {
    for (const auto& [name, _] : signature_.variables()) {
      bindings_.insert({name, nullptr});
    }
  }

  determineUnboundedTypeVariables();
  for (auto i = 0; i < formalArgsCnt; i++) {
    TypePtr actualArg;
    if (formalArgs[i].baseName() == "any") {
      actualArg = randType();
    } else {
      actualArg = exec::SignatureBinder::tryResolveType(
          formalArgs[i], variables(), bindings_);
      BOLT_CHECK(actualArg != nullptr);
    }
    argumentTypes_.push_back(actualArg);
  }

  // Generate random repeats of the last argument type if the signature is
  // variadic.
  if (signature_.variableArity()) {
    auto repeat = boost::random::uniform_int_distribution<uint32_t>(
        0, maxVariadicArgs)(rng_);
    auto last = argumentTypes_[formalArgsCnt - 1];
    for (int i = 0; i < repeat; ++i) {
      argumentTypes_.push_back(last);
    }
  }

  return true;
}

TypePtr ArgumentTypeFuzzer::fuzzReturnType() {
  BOLT_CHECK_EQ(
      returnType_,
      nullptr,
      "Only fuzzing uninitialized return type is allowed.");

  determineUnboundedTypeVariables();
  if (signature_.returnType().baseName() == "any") {
    returnType_ = randType();
    return returnType_;
  } else {
    returnType_ = exec::SignatureBinder::tryResolveType(
        signature_.returnType(), variables(), bindings_);
    BOLT_CHECK_NE(returnType_, nullptr);
    return returnType_;
  }
}

} // namespace bytedance::bolt::fuzzer
