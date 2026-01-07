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
#include <atomic>
#include <functional>
#include <memory>
namespace bytedance::bolt::exec {
class LocalExchangeQueue;

class PushBasedEvent {
 public:
  PushBasedEvent(
      std::function<
          void(std::shared_ptr<LocalExchangeQueue>, const int, const bool)>
          create_consuming_driver_callback,
      std::function<void(const bool)> finish_driver_creator_callback)
      : create_consuming_driver_callback_{create_consuming_driver_callback},
        finish_driver_creator_callback_{
            finish_driver_creator_callback,
        } {};

  // Create and schedule a new driver for the downstream pipeline under
  // push-based mode. In case for the last driver to be scheduled, make `isLast`
  // true.
  bool schedule(
      const std::shared_ptr<LocalExchangeQueue>& inputQueue,
      int partition,
      bool isLast = false);
  bool finish(bool isLast = false);

 private:
  // Callback function for creating drivers for the consuming pipeline
  std::function<
      void(std::shared_ptr<LocalExchangeQueue>, const int, const bool)>
      create_consuming_driver_callback_{nullptr};

  // Callback function for optionally ending ParitionedOutputBuffer at the end
  // of the pipeline
  std::function<void(const bool)> finish_driver_creator_callback_{nullptr};

  // Flag to check if the "finish" has been already called, since it can be only
  // called once.
  std::atomic<bool> ifFinished_{false};
};
} // namespace bytedance::bolt::exec