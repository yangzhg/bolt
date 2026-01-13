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

#include "PushBasedEvent.h"
namespace bytedance::bolt::exec {

bool PushBasedEvent::schedule(
    const std::shared_ptr<LocalExchangeQueue>& inputQueue,
    int partition,
    bool isLast) {
  if (isLast) {
    if (!finish(isLast)) {
      return false;
    }
  }
  create_consuming_driver_callback_(inputQueue, partition, isLast);
  return true;
}

bool PushBasedEvent::finish(bool isLast) {
  bool expected = false;
  if (ifFinished_.compare_exchange_strong(expected, true)) {
    finish_driver_creator_callback_(isLast);
    return true;
  }
  return false;
}
} // namespace bytedance::bolt::exec
