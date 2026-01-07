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

#include <mutex>
#include <string>
#include <vector>
namespace bytedance::bolt::process {

class ExceptionTraceContext {
 public:
  static ExceptionTraceContext& get_instance();
  ExceptionTraceContext(ExceptionTraceContext const&) = delete;
  void operator=(ExceptionTraceContext const&) = delete;
  // as exception' name is not large, so we can think there are no exceptions.
  static std::string get_exception_name(const void* info);
  bool prefix_in_black_list(const std::string& exception) const;
  bool prefix_in_white_list(const std::string& exception) const;
  int get_level() const;
  void set_level(int level);

  // prefix with the namespace of expressions, delimited by comma.
  void set_whitelist(const std::string& white_list);

 private:
  ExceptionTraceContext();
  ~ExceptionTraceContext(){};
  std::vector<std::string> white_list_;
  std::vector<std::string> black_list_;
  int level_;
  std::mutex mutex_;
};

} // namespace bytedance::bolt::process