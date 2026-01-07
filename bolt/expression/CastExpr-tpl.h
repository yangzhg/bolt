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

#include "bolt/expression/EvalCtx.h"
#include "bolt/functions/InlineFlatten.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/SelectivityVector.h"
#include "folly/Conv.h"
namespace bytedance::bolt::exec {
namespace CastUtils {

enum class CastErrorPolicy {
  // throw exceptions on errors
  ThrowOnFailure,
  // return null on errors
  NullOnFailure,
  // compatible with spark's cast, will return null on most errors,
  // exceptions:
  //   1. integer overflow, wrap around
  //   2. string with point to integer, truncate the point part
  SparkCastPolicy
};

void doCast(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType,
    VectorPtr& result,
    CastErrorPolicy errorPolicy);

} // namespace CastUtils
} // namespace bytedance::bolt::exec