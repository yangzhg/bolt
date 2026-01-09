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

#include "bolt/common/base/BoltLibSignalHandler.h"
#include "bolt/common/base/Exceptions.h"

#if defined(__APPLE__)
#define BOOST_STACKTRACE_GNU_SOURCE_NOT_REQUIRED
#endif

#include <boost/stacktrace.hpp>
#include <folly/system/ThreadName.h>

#include <signal.h>

#include <mutex>
#include <string>

// Must not exceed 15 characters.
const std::string BoltLibThreadNamePrefix = "BoltThread";
// Sometimes we get signals from JVM that we are not supposed to handle.
// We differentiate these signals by examining the stacksize, which in Bolt
// driver is usually much more than 10 frames.
uint64_t ReasonableBoltStackSize = 0;

struct {
  int number;
  const char* name;
  struct sigaction oldAction;
} kFatalSignals[] = {
    // Only synchronous signals (SEGV, ILL, FPE) are supported.
    // Async signals are handled by the containing runtime, e.g. SIGTERM.
    {SIGSEGV, "SIGSEGV", {}},
    {SIGILL, "SIGILL", {}},
    {SIGFPE, "SIGFPE", {}},
    {SIGABRT, "SIGABRT", {}},
    {0, nullptr, {}},
};

void callOldSignalHandler(int signum, siginfo_t* info, void* uctx) {
  for (auto p = kFatalSignals; p->name; ++p) {
    if (p->number == signum) {
      if (p->oldAction.sa_flags & SA_SIGINFO) {
        p->oldAction.sa_sigaction(signum, info, uctx);
        return;
      }
      sigaction(signum, &p->oldAction, nullptr);
      raise(signum);
      return;
    }
  }

  // Not one of the signals we know about. Oh well. Reset to default.
  struct sigaction sa {};
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = SIG_DFL;
  sigaction(signum, &sa, nullptr);
  raise(signum);
}

void boltLibSignalHandler(int signum, siginfo_t* info, void* uctx) {
  BOLT_FAIL("Internal Bolt Error");
}

bool shouldDispatchBoltHandler() {
  auto curThreadName = folly::getCurrentThreadName();

  bool isBoltLibThread = curThreadName.hasValue() &&
      curThreadName.value().find(BoltLibThreadNamePrefix) != std::string::npos;

  auto stack = boost::stacktrace::stacktrace();

  // Sometimes, we receive asynchronous signals or really bad situations of
  // really short stack that clearly is not the stack of a bolt task executing.
  // isReasonableStackSize is a broadstroke way to prune out such cases.
  bool isReasonableStackSize =
      stack.as_vector().size() > ReasonableBoltStackSize;
  return isBoltLibThread && isReasonableStackSize;
}

void signalHandler(int signum, siginfo_t* info, void* uctx) {
  if (shouldDispatchBoltHandler()) {
    boltLibSignalHandler(signum, info, uctx);
  } else {
    callOldSignalHandler(signum, info, uctx);
  }
}

std::mutex installationGuard;
bool signalHandlerInstalled = false;

void installSignalHandler(uint64_t stackSize) {
  std::lock_guard<std::mutex> guard(installationGuard);
  if (signalHandlerInstalled) {
    return;
  }
  signalHandlerInstalled = true;
  ReasonableBoltStackSize = stackSize;

  // Install signal handler
  struct sigaction sa {};
  memset(&sa, 0, sizeof(sa));

  // on entry, we do not want any signal masked.
  sigfillset(&sa.sa_mask);

  // By default signal handlers are run on the signaled thread's stack.
  // In case of stack overflow running the SIGSEGV signal handler on
  // the same stack leads to another SIGSEGV and crashes the program.
  // Use SA_ONSTACK, so alternate stack is used (only if configured via
  // sigaltstack).
  // Golang also requires SA_ONSTACK. See:
  // https://golang.org/pkg/os/signal/#hdr-Go_programs_that_use_cgo_or_SWIG
  sa.sa_flags |= SA_SIGINFO | SA_ONSTACK;
  sa.sa_sigaction = &signalHandler;

  for (auto p = kFatalSignals; p->name; ++p) {
    sigaction(p->number, &sa, &p->oldAction);
  }
}

void triggerSegfault() {
  raise(SIGSEGV);
}
