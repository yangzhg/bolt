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

#include "bolt/functions/colocate/client/UDFClientManager.h"
#include <memory>
namespace bytedance::bolt::functions {

std::shared_ptr<UDFClient> UDFClientManager::AcquireRandom() const {
  return pool_->acquireClient();
}

std::vector<std::shared_ptr<UDFClient>> UDFClientManager::AcquireAll() {
  std::vector<std::shared_ptr<UDFClient>> clients;
  for (auto index = 0; index < udf_hosts_.size(); ++index) {
    auto client =
        std::make_shared<UDFClient>(udf_hosts_[index], udf_ports_[index]);
    clients.push_back(client);
  }
  return clients;
}

void UDFClientManager::Release(std::shared_ptr<UDFClient>& client) {
  pool_->releaseClient(client);
}

void UDFClientManager::UpdateServers(
    std::vector<std::string> udf_hosts,
    std::vector<int> udf_ports) {
  BOLT_CHECK_EQ(udf_hosts.size(), udf_ports.size());
  pool_ = std::make_shared<UDFClientPool>(pool_size_, udf_hosts, udf_ports);
}
} // namespace bytedance::bolt::functions
