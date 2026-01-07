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

#include <execinfo.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include "bolt/common/process/ExceptionTrace.h"
#include "bolt/common/process/ExceptionTraceContext.h"
#if ENABLE_EXCEPTION_TRACE
using namespace bytedance::bolt::process;

class ExceptionTraceTest : public testing::Test {
 public:
  ExceptionTraceTest() = default;
  ~ExceptionTraceTest() override = default;
  void SetUp() override {
    _old = std::cerr.rdbuf(_buffer.rdbuf());
  }
  void TearDown() override {
    std::cerr.rdbuf(_old);
  }

  std::streambuf* _old = nullptr;
  std::stringstream _buffer;
};

TEST_F(ExceptionTraceTest, DISABLED_print_exception_stack) {
  std::string exception = "runtime_error";
  std::string res;
  try {
    throw std::runtime_error("test_print_exception_stack.");
  } catch (std::exception& e) {
    res = e.what();
  } catch (...) {
    res = "unknown";
  }
  std::string text = _buffer.str();
  ASSERT_TRUE(text.find(exception) != text.npos);
}
#endif