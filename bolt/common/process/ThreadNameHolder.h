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

#include <folly/system/ThreadName.h>
namespace bytedance::bolt::process {
class ThreadNameHolder {
 public:
  explicit ThreadNameHolder(std::string_view task_id)
      : origin_thread_name(folly::getCurrentThreadName().value_or("idle")) {
    if (task_id.size() > 16 && task_id.size() < 27) {
      thread_name = task_id.substr(BEGIN_POS);
    } else if (task_id.size() >= 27) {
      thread_name = task_id.substr(BEGIN_POS, FIRST_PART_SIZE);
      thread_name +=
          task_id.substr(BEGIN_POS + FIRST_PART_SIZE + 1, SECOND_PART_SIZE);
      thread_name += task_id.substr(
          BEGIN_POS + FIRST_PART_SIZE + 1 + SECOND_PART_SIZE + 1,
          THIRD_PART_SIZE);
    } else if (task_id.size() > 0) {
      thread_name = task_id;
    } else {
      needRecovery = false;
      return;
    }
    folly::setThreadName(thread_name);
  }

  ~ThreadNameHolder() {
    if (needRecovery) {
      folly::setThreadName(origin_thread_name);
    }
  }

 private:
  static constexpr size_t BEGIN_POS = 9;
  static constexpr size_t FIRST_PART_SIZE = 6;
  static constexpr size_t SECOND_PART_SIZE = 5;
  static constexpr size_t THIRD_PART_SIZE = 5;
  const std::string origin_thread_name;
  bool needRecovery = true;
  std::string thread_name;
};
} // namespace bytedance::bolt::process
