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

#include "bolt/connectors/Connector.h"
#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/config/Config.h"

#include <gtest/gtest.h>
namespace bytedance::bolt::connector {

class ConnectorTest : public testing::Test {};

namespace {

class TestConnector : public connector::Connector {
 public:
  TestConnector(const std::string& id) : connector::Connector(id) {}

  std::unique_ptr<connector::DataSource> createDataSource(
      const RowTypePtr& /* outputType */,
      const std::shared_ptr<ConnectorTableHandle>& /* tableHandle */,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& /* columnHandles */,
      std::shared_ptr<ConnectorQueryCtx> connectorQueryCtx,
      const core::QueryConfig& /* queryConfig */) override {
    BOLT_NYI();
  }

  std::unique_ptr<connector::DataSink> createDataSink(
      RowTypePtr /*inputType*/,
      std::shared_ptr<
          ConnectorInsertTableHandle> /*connectorInsertTableHandle*/,
      ConnectorQueryCtx* /*connectorQueryCtx*/,
      CommitStrategy /*commitStrategy*/,
      const core::QueryConfig& /*queryConfig*/) override final {
    BOLT_NYI();
  }
};

class TestConnectorFactory : public connector::ConnectorFactory {
 public:
  static constexpr const char* kConnectorFactoryName = "test-factory";

  TestConnectorFactory() : ConnectorFactory(kConnectorFactoryName) {}

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* executor = nullptr) override {
    return std::make_shared<TestConnector>(id);
  }

  std::shared_ptr<Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const Config> /*config*/,
      folly::Executor* /*executor*/ = nullptr) override {
    return std::make_shared<TestConnector>(id);
  }
};

} // namespace

TEST_F(ConnectorTest, getAllConnectors) {
  registerConnectorFactory(std::make_shared<TestConnectorFactory>());
  BOLT_ASSERT_THROW(
      registerConnectorFactory(std::make_shared<TestConnectorFactory>()),
      "ConnectorFactory with name 'test-factory' is already registered");
  EXPECT_TRUE(hasConnectorFactory(TestConnectorFactory::kConnectorFactoryName));
  const int32_t numConnectors = 10;
  for (int32_t i = 0; i < numConnectors; i++) {
    registerConnector(
        getConnectorFactory(TestConnectorFactory::kConnectorFactoryName)
            ->newConnector(
                fmt::format("connector-{}", i),
                std::shared_ptr<const config::ConfigBase>{}));
  }
  const auto& connectors = getAllConnectors();
  EXPECT_EQ(connectors.size(), numConnectors);
  for (int32_t i = 0; i < numConnectors; i++) {
    EXPECT_EQ(connectors.count(fmt::format("connector-{}", i)), 1);
  }
  for (int32_t i = 0; i < numConnectors; i++) {
    unregisterConnector(fmt::format("connector-{}", i));
  }
  EXPECT_EQ(getAllConnectors().size(), 0);
  EXPECT_TRUE(
      unregisterConnectorFactory(TestConnectorFactory::kConnectorFactoryName));
  EXPECT_FALSE(
      unregisterConnectorFactory(TestConnectorFactory::kConnectorFactoryName));
}
} // namespace bytedance::bolt::connector
