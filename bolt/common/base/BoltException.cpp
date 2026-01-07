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

#include "bolt/common/base/BoltException.h"

#include <folly/synchronization/AtomicStruct.h>
#include <exception>
namespace bytedance {
namespace bolt {

std::exception_ptr toBoltException(const std::exception_ptr& exceptionPtr) {
  try {
    std::rethrow_exception(exceptionPtr);
  } catch (const BoltException&) {
    return exceptionPtr;
  } catch (const std::exception& e) {
    return std::make_exception_ptr(
        BoltUserError(std::current_exception(), e.what(), false));
  }
}

int64_t& threadNumBoltThrow() {
  thread_local int64_t numThrow = 0;
  return numThrow;
}

bool& threadSkipErrorDetails() {
  thread_local bool skipErrorDetails{false};
  return skipErrorDetails;
}

ExceptionContext& getExceptionContext() {
  thread_local ExceptionContext context;
  return context;
}

// Traverses the context hierarchy and appends messages from all contexts that
// are marked as essential.
std::string getAdditionalExceptionContextString(
    BoltException::Type exceptionType,
    const std::string& currentMessage) {
  auto* context = &getExceptionContext();
  std::string additionalMessage = "";
  if (!context->parent || !context->parent->parent) {
    return additionalMessage;
  }
  context = context->parent;
  while (context->parent) {
    if (context->isEssential) {
      auto message = context->message(exceptionType);
      if (!message.empty()) {
        additionalMessage += message + " ";
      }
    }
    context = context->parent;
  }
  if (!additionalMessage.empty()) {
    // Get rid of the extra space at the end.
    additionalMessage.pop_back();
  }
  return additionalMessage;
}

BoltException::BoltException(
    const char* file,
    size_t line,
    const char* function,
    std::string_view failingExpression,
    std::string_view message,
    std::string_view errorSource,
    std::string_view errorCode,
    bool isRetriable,
    Type exceptionType,
    std::string_view exceptionName)
    : BoltException(State::make(exceptionType, [&](auto& state) {
        state.exceptionType = exceptionType;
        state.exceptionName = exceptionName;
        state.file = file;
        state.line = line;
        state.function = function;
        state.failingExpression = failingExpression;
        state.message = message;
        state.errorSource = errorSource;
        state.errorCode = errorCode;
        state.context = getExceptionContext().message(exceptionType);
        state.additionalContext =
            getAdditionalExceptionContextString(exceptionType, state.context);
        state.isRetriable = isRetriable;
      })) {}

BoltException::BoltException(
    const std::exception_ptr& e,
    std::string_view message,
    std::string_view errorSource,
    std::string_view errorCode,
    bool isRetriable,
    Type exceptionType,
    std::string_view exceptionName)
    : BoltException(State::make([&](auto& state) {
        state.exceptionType = exceptionType;
        state.exceptionName = exceptionName;
        state.file = "UNKNOWN";
        state.line = 0;
        state.function = "";
        state.failingExpression = "";
        state.message = message;
        state.errorSource = errorSource;
        state.errorCode = errorCode;
        state.context = getExceptionContext().message(exceptionType);
        state.additionalContext =
            getAdditionalExceptionContextString(exceptionType, state.context);
        state.isRetriable = isRetriable;
        state.wrappedException = e;
      })) {}

namespace {

/// returns whether BoltException stacktraces are enabled and whether, if they
/// are rate-limited, whether the rate-limit check passes
bool isStackTraceEnabled(BoltException::Type type) {
  using namespace std::literals::chrono_literals;
  const bool isSysException = type == BoltException::Type::kSystem;
  if ((isSysException && !FLAGS_bolt_exception_system_stacktrace_enabled) ||
      (!isSysException && !FLAGS_bolt_exception_user_stacktrace_enabled)) {
    // BoltException stacktraces are disabled.
    return false;
  }

  const int32_t rateLimitMs = isSysException
      ? FLAGS_bolt_exception_system_stacktrace_rate_limit_ms
      : FLAGS_bolt_exception_user_stacktrace_rate_limit_ms;
  // not static so the gflag can be manipulated at runtime
  if (0 == rateLimitMs) {
    // BoltException stacktraces are not rate-limited
    return true;
  }
  static folly::AtomicStruct<std::chrono::steady_clock::time_point> systemLast;
  static folly::AtomicStruct<std::chrono::steady_clock::time_point> userLast;
  auto* last = isSysException ? &systemLast : &userLast;

  auto const now = std::chrono::steady_clock::now();
  auto latest = last->load(std::memory_order_relaxed);
  if (now < latest + std::chrono::milliseconds(rateLimitMs)) {
    // BoltException stacktraces are rate-limited and the rate-limit check
    // failed
    return false;
  }

  // BoltException stacktraces are rate-limited and the rate-limit check
  // passed
  //
  // the cas happens only here, so the rate-limit check in effect gates not
  // only computation of the stacktrace but also contention on this atomic
  // variable
  return last->compare_exchange_strong(latest, now, std::memory_order_relaxed);
}

} // namespace

template <typename F>
std::shared_ptr<const BoltException::State> BoltException::State::make(
    BoltException::Type exceptionType,
    F f) {
  auto state = std::make_shared<BoltException::State>();
  if (isStackTraceEnabled(exceptionType)) {
    // new v.s. make_unique to avoid any extra frames from make_unique
    state->stackTrace.reset(new process::StackTrace());
  }
  f(*state);
  return state;
}

/*
Not much to say. Constructs the elaborate message from the available
pieces of information.
 */
void BoltException::State::finalize() const {
  assert(elaborateMessage.empty());

  // Fill elaborateMessage_
  if (!exceptionName.empty()) {
    elaborateMessage += "Exception: ";
    elaborateMessage += exceptionName;
    elaborateMessage += '\n';
  }

  if (!errorSource.empty()) {
    elaborateMessage += "Error Source: ";
    elaborateMessage += errorSource;
    elaborateMessage += '\n';
  }

  if (!errorCode.empty()) {
    elaborateMessage += "Error Code: ";
    elaborateMessage += errorCode;
    elaborateMessage += '\n';
  }

  if (!message.empty()) {
    elaborateMessage += "Reason: ";
    elaborateMessage += message;
    elaborateMessage += '\n';
  }

  elaborateMessage += "Retriable: ";
  elaborateMessage += isRetriable ? "True" : "False";
  elaborateMessage += '\n';

  if (!failingExpression.empty()) {
    elaborateMessage += "Expression: ";
    elaborateMessage += failingExpression;
    elaborateMessage += '\n';
  }

  if (!context.empty()) {
    elaborateMessage += "Context: " + context + "\n";
  }

  if (!additionalContext.empty()) {
    elaborateMessage += "Additional Context: " + additionalContext + "\n";
  }

  if (function) {
    elaborateMessage += "Function: ";
    elaborateMessage += function;
    elaborateMessage += '\n';
  }

  if (file) {
    elaborateMessage += "File: ";
    elaborateMessage += file;
    elaborateMessage += '\n';
  }

  if (line) {
    elaborateMessage += "Line: ";
    auto len = elaborateMessage.size();
    size_t t = line;
    do {
      elaborateMessage += static_cast<char>('0' + t % 10);
      t /= 10;
    } while (t);
    reverse(elaborateMessage.begin() + len, elaborateMessage.end());
    elaborateMessage += '\n';
  }

  elaborateMessage += "Stack trace:\n";
  if (stackTrace) {
    bool demangleSym = false;
#ifndef NDEBUG
    demangleSym = true;
#endif
    elaborateMessage += stackTrace->toString(demangleSym);
  } else {
    elaborateMessage += "Stack trace has been disabled.";
    if (exceptionType == BoltException::Type::kSystem) {
      elaborateMessage +=
          " Use --bolt_exception_system_stacktrace_enabled=true to enable it.\n";
    } else {
      elaborateMessage +=
          " Use --bolt_exception_user_stacktrace_enabled=true to enable it.\n";
    }
  }
}

const char* BoltException::State::what() const noexcept {
  try {
    folly::call_once(once, [&] { finalize(); });
    return elaborateMessage.c_str();
  } catch (...) {
    return "<unknown failure in BoltException::what>";
  }
}

} // namespace bolt
} // namespace bytedance
