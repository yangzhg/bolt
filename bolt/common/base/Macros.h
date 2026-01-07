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

// Macros to disable deprecation warnings
#ifdef __clang__
#define BOLT_SUPPRESS_DEPRECATION_WARNING \
  _Pragma("clang diagnostic push");       \
  _Pragma("clang diagnostic ignored \"-Wdeprecated-declarations\"")
#define BOLT_UNSUPPRESS_DEPRECATION_WARNING _Pragma("clang diagnostic pop");
#define BOLT_SUPPRESS_RETURN_LOCAL_ADDR_WARNING
#define BOLT_UNSUPPRESS_RETURN_LOCAL_ADDR_WARNING
#else
#define BOLT_SUPPRESS_DEPRECATION_WARNING
#define BOLT_UNSUPPRESS_DEPRECATION_WARNING
#define BOLT_SUPPRESS_RETURN_LOCAL_ADDR_WARNING \
  _Pragma("GCC diagnostic push");               \
  _Pragma("GCC diagnostic ignored \"-Wreturn-local-addr\"")
#define BOLT_UNSUPPRESS_RETURN_LOCAL_ADDR_WARNING _Pragma("GCC diagnostic pop");
#endif

#define BOLT_CONCAT(x, y) x##y
// Need this extra layer to expand __COUNTER__.
#define BOLT_VARNAME_IMPL(x, y) BOLT_CONCAT(x, y)
#define BOLT_VARNAME(x) BOLT_VARNAME_IMPL(x, __COUNTER__)
