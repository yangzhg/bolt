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

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/Exceptions.h"
namespace bytedance {
namespace bolt {
namespace dwio {
namespace common {
namespace exception {

class ExceptionLogger {
 public:
  virtual ~ExceptionLogger() = default;

  virtual void logException(
      const char* file,
      size_t line,
      const char* function,
      const char* expression,
      const char* message) = 0;

  virtual void logWarning(
      const char* file,
      size_t line,
      const char* function,
      const char* expression,
      const char* message) = 0;
};

bool registerExceptionLogger(std::unique_ptr<ExceptionLogger> logger);

ExceptionLogger* getExceptionLogger();

class LoggedException : public bolt::BoltException {
 public:
  explicit LoggedException(
      const std::string& errorMessage,
      const std::string& errorSource =
          ::bytedance::bolt::error_source::kErrorSourceRuntime,
      const std::string& errorCode = ::bytedance::bolt::error_code::kUnknown,
      const bool isRetriable = false)
      : BoltException(
            nullptr,
            0,
            nullptr,
            "",
            errorMessage,
            errorSource,
            errorCode,
            isRetriable) {
    logException();
  }

  LoggedException(
      const char* file,
      size_t line,
      const char* function,
      const char* expression,
      const std::string& errorMessage,
      const std::string& errorSource,
      const std::string& errorCode)
      : BoltException(
            file,
            line,
            function,
            expression,
            errorMessage,
            errorSource,
            errorCode,
            false) {
    logException();
  }

 private:
  void logException() {
    auto logger = getExceptionLogger();
    if (logger) {
      logger->logException(
          file(),
          line(),
          function(),
          failingExpression().data(),
          BoltException::what());
    }
  }
};

class DecompressionLoggedException : public LoggedException {
 public:
  explicit DecompressionLoggedException(
      const char* file,
      size_t line,
      const char* function,
      const char* expression,
      const std::string& errorMessage,
      const std::string& errorSource,
      const std::string& errorCode)
      : LoggedException(
            file,
            line,
            function,
            expression,
            errorMessage,
            errorSource,
            errorCode) {}
};

} // namespace exception
} // namespace common

#define DWIO_WARN_IF(e, ...)                                                \
  ({                                                                        \
    auto const& _tmp = (e);                                                 \
    if (_tmp) {                                                             \
      auto logger =                                                         \
          ::bytedance::bolt::dwio::common::exception::getExceptionLogger(); \
      if (logger) {                                                         \
        logger->logWarning(                                                 \
            __FILE__,                                                       \
            __LINE__,                                                       \
            __FUNCTION__,                                                   \
            #e,                                                             \
            ::folly::to<std::string>(__VA_ARGS__).c_str());                 \
      }                                                                     \
    }                                                                       \
  })

#define DWIO_WARN(...) DWIO_WARN_IF(true, ##__VA_ARGS__)

#define DWIO_WARN_EVERY_N(n, ...)          \
  static size_t LOG_OCCURRENCES_MOD_N = 0; \
  if (++LOG_OCCURRENCES_MOD_N > n)         \
    LOG_OCCURRENCES_MOD_N -= n;            \
  if (LOG_OCCURRENCES_MOD_N == 1)          \
    DWIO_WARN(__VA_ARGS__);

/*
 * Use ENFORCE(expr) or ENFORCE(expr, message) whenever you want to
 * make sure that expr is nonzero. If it is zero, an exception is
 * thrown. The type yielded by ENFORCE is the same as expr, so it acts
 * like an identity function, which makes it convenient to insert
 * anywhere. For example:
 *
 * auto file = ENFORCE(fopen("file.txt", "r"), "Can't find file.txt");
 * auto p = ENFORCE(dynamic_cast<Type*>(pointer));
 *
 * In case the expression is nonzero, the message, if any, is not
 * evaluated so you can plant in there a costly string concatenation:
 *
 * string fn = "file.txt"
 * auto file = ENFORCE(fopen(fn.c_str(), "r"), "Can't find " + fn);
 *
 * The ENFORCE macro stores the file, name, and function into the
 * FBException thrown.
 */
#define DWIO_ENFORCE_CUSTOM(                            \
    exception, expression, errorSource, errorCode, ...) \
  ({                                                    \
    auto const& _tmp = (expression);                    \
    _tmp ? _tmp                                         \
         : throw exception(                             \
               __FILE__,                                \
               __LINE__,                                \
               __FUNCTION__,                            \
               #expression,                             \
               ::folly::to<std::string>(__VA_ARGS__),   \
               errorSource,                             \
               errorCode);                              \
  })

/*
Unconditionally throws an exception derived from BoltException
containing information about the file, line, and function where it happened.
 */
#define DWIO_EXCEPTION_CUSTOM(exception, errorSource, errorCode, ...) \
  (throw exception(                                                   \
      __FILE__,                                                       \
      __LINE__,                                                       \
      __FUNCTION__,                                                   \
      "",                                                             \
      ::folly::to<std::string>(__VA_ARGS__),                          \
      errorSource,                                                    \
      errorCode))

#define DWIO_RAISE(...)                                          \
  DWIO_EXCEPTION_CUSTOM(                                         \
      bytedance::bolt::dwio::common::exception::LoggedException, \
      ::bytedance::bolt::error_source::kErrorSourceRuntime,      \
      ::bytedance::bolt::error_code::kUnknown,                   \
      ##__VA_ARGS__)

#define DWIO_ENSURE(expr, ...)                                   \
  DWIO_ENFORCE_CUSTOM(                                           \
      bytedance::bolt::dwio::common::exception::LoggedException, \
      expr,                                                      \
      ::bytedance::bolt::error_source::kErrorSourceRuntime,      \
      ::bytedance::bolt::error_code::kUnknown,                   \
      ##__VA_ARGS__)

#define DECOMPRESSION_ENSURE(expr, ...)                                   \
  if (UNLIKELY(!(expr))) {                                                \
    /* GCC 9.2.1 doesn't accept this code with constexpr. */              \
    auto message = ::bytedance::bolt::detail::errorMessage(__VA_ARGS__);  \
    static_assert(                                                        \
        !std::is_same_v<                                                  \
            typename ::bytedance::bolt::detail::BoltCheckFailStringType<  \
                decltype(message)>::type,                                 \
            std::string>,                                                 \
        "BUG: we should not pass std::string by value to boltCheckFail"); \
    throw ::bytedance::bolt::dwio::common::exception::                    \
        DecompressionLoggedException(                                     \
            __FILE__,                                                     \
            __LINE__,                                                     \
            __FUNCTION__,                                                 \
            #expr,                                                        \
            message,                                                      \
            ::bytedance::bolt::error_source::kErrorSourceRuntime.c_str(), \
            ::bytedance::bolt::error_code::kInvalidState.c_str());        \
  }

#define DWIO_ENSURE_NOT_NULL(p, ...) \
  DWIO_ENSURE(p != nullptr, "[Null pointer] : ", ##__VA_ARGS__);

#define DWIO_ENSURE_NE(l, r, ...)       \
  DWIO_ENSURE(                          \
      l != r,                           \
      "[Range Constraint Violation : ", \
      l,                                \
      "!=",                             \
      r,                                \
      "] : ",                           \
      ##__VA_ARGS__);

#define DWIO_ENSURE_EQ(l, r, ...)       \
  DWIO_ENSURE(                          \
      l == r,                           \
      "[Range Constraint Violation : ", \
      l,                                \
      "==",                             \
      r,                                \
      "] : ",                           \
      ##__VA_ARGS__);

#define DWIO_ENSURE_LT(l, r, ...)       \
  DWIO_ENSURE(                          \
      l < r,                            \
      "[Range Constraint Violation : ", \
      l,                                \
      "<",                              \
      r,                                \
      "] : ",                           \
      ##__VA_ARGS__);

#define DWIO_ENSURE_LE(l, r, ...)       \
  DWIO_ENSURE(                          \
      l <= r,                           \
      "[Range Constraint Violation : ", \
      l,                                \
      "<=",                             \
      r,                                \
      "] : ",                           \
      ##__VA_ARGS__);

#define DWIO_ENSURE_GT(l, r, ...)       \
  DWIO_ENSURE(                          \
      l > r,                            \
      "[Range Constraint Violation : ", \
      l,                                \
      ">",                              \
      r,                                \
      "] : ",                           \
      ##__VA_ARGS__);

#define DWIO_ENSURE_GE(l, r, ...)       \
  DWIO_ENSURE(                          \
      l >= r,                           \
      "[Range Constraint Violation : ", \
      l,                                \
      ">=",                             \
      r,                                \
      "] : ",                           \
      ##__VA_ARGS__);

} // namespace dwio
} // namespace bolt
} // namespace bytedance
