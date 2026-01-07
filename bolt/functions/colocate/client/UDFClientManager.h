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
#include <random>
#include <string>
#include <vector>
#include "bolt/common/base/Exceptions.h"
#include "bolt/functions/colocate/client/UDFClient.h"
#include "bolt/functions/colocate/client/UDFClientPool.h"
namespace bytedance::bolt::functions {
class UDFClientManager {
 public:
  UDFClientManager(
      const std::vector<std::string>& udf_hosts,
      const std::vector<int>& udf_ports,
      const unsigned int pool_size)
      : udf_hosts_(udf_hosts), udf_ports_(udf_ports), pool_size_(pool_size) {
    BOLT_CHECK_EQ(udf_hosts.size(), udf_ports.size());
    pool_ = std::make_shared<UDFClientPool>(pool_size, udf_hosts, udf_ports);
  }

  std::shared_ptr<UDFClient> AcquireRandom() const;

  std::vector<std::shared_ptr<UDFClient>> AcquireAll();

  void Release(std::shared_ptr<UDFClient>&);

  void UpdateServers(std::vector<std::string>, std::vector<int>);

 private:
  std::vector<std::string> udf_hosts_;

  std::vector<int> udf_ports_;

  const unsigned int pool_size_;

  std::shared_ptr<UDFClientPool> pool_;
};
} // namespace bytedance::bolt::functions
