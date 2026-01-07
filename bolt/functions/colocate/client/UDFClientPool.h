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

#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <string>

#include "bolt/common/base/Exceptions.h"
#include "bolt/functions/colocate/client/UDFClient.h"
namespace bytedance::bolt::functions {
class UDFClientPool {
 public:
  /*
   * multiple server cases: if the UDF server is remote, there usually have
   * multiple UDF servers that bolt can connect.
   */
  UDFClientPool(
      unsigned int pool_size,
      const std::vector<std::string>& hosts,
      const std::vector<int>& ports) {
    BOLT_CHECK_EQ(hosts.size(), ports.size());
    std::random_device rd; // Corrected random_device declaration
    std::mt19937 gen(rd());
    auto size = hosts.size();
    std::uniform_int_distribution<std::size_t> dist(0, size - 1);
    for (unsigned int i = 0; i < pool_size; ++i) {
      std::size_t random_index = dist(gen);
      auto hostname = hosts[random_index];
      auto port = ports[random_index];
      auto client = std::make_shared<UDFClient>(hostname, port);
      clients_.push_back(std::move(client));
    }
  }

  std::shared_ptr<UDFClient> acquireClient();

  void releaseClient(std::shared_ptr<UDFClient> client);

 private:
  std::vector<std::shared_ptr<UDFClient>> clients_;

  std::mutex mutex_;

  std::condition_variable cv_;
};
} // namespace bytedance::bolt::functions
