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

#include "HdfsMiniCluster.h"
namespace bytedance::bolt::filesystems::test {
void HdfsMiniCluster::start() {
  try {
    serverProcess_ = std::make_unique<boost::process::child>(
        env_,
        exePath_,
        jarCommand,
        env_["HADOOP_HOME"].to_string() + miniclusterJar,
        miniclusterCommand,
        noMapReduceOption,
        formatNameNodeOption,
        httpPortOption,
        httpPort,
        nameNodePortOption,
        nameNodePort,
        configurationOption,
        turnOffPermissions);

    auto future =
        std::async(std::launch::async, [&]() { serverProcess_->wait(); });

    if (future.wait_for(std::chrono::milliseconds(60000)) ==
        std::future_status::timeout) {
      serverProcess_->terminate();
      throw std::runtime_error("Process wait timeout");
    }

    BOLT_CHECK_EQ(
        serverProcess_->exit_code(),
        383,
        "Minicluster process exited, code: ",
        serverProcess_->exit_code())
  } catch (const std::exception& e) {
    BOLT_FAIL("Failed to launch Minicluster server: {}", e.what());
  }
}

void HdfsMiniCluster::stop() {
  if (serverProcess_ && serverProcess_->valid()) {
    serverProcess_->terminate();
    serverProcess_->wait();
    serverProcess_.reset();
  }
}

bool HdfsMiniCluster::isRunning() {
  if (serverProcess_) {
    return true;
  }
  return false;
}

// requires hadoop executable to be on the PATH
HdfsMiniCluster::HdfsMiniCluster() {
  env_ = (boost::process::environment)boost::this_process::environment();
  env_["PATH"] = env_["PATH"].to_string() + hadoopSearchPath;
  auto path = env_["PATH"].to_vector();
  exePath_ = boost::process::search_path(
      miniClusterExecutableName,
      std::vector<boost::filesystem::path>(path.begin(), path.end()));
  if (exePath_.empty()) {
    BOLT_FAIL(
        "Failed to find minicluster executable {}'", miniClusterExecutableName);
  }
  boost::filesystem::path hadoopHomeDirectory = exePath_;
  hadoopHomeDirectory.remove_filename().remove_filename();
  setupEnvironment(hadoopHomeDirectory.string());
}

void HdfsMiniCluster::addFile(std::string source, std::string destination) {
  auto filePutProcess = std::make_shared<boost::process::child>(
      env_,
      exePath_,
      filesystemCommand,
      filesystemUrlOption,
      filesystemUrl,
      filePutOption,
      source,
      destination);

  auto future =
      std::async(std::launch::async, [&]() { filePutProcess->wait(); });
  bool isExited = future.wait_for(std::chrono::milliseconds(5000)) ==
      std::future_status::ready;

  if (!isExited) {
    BOLT_FAIL(
        "Failed to add file to hdfs, exit code: {}",
        filePutProcess->exit_code())
  }
}

HdfsMiniCluster::~HdfsMiniCluster() {
  stop();
}

void HdfsMiniCluster::setupEnvironment(const std::string& homeDirectory) {
  env_["HADOOP_HOME"] = homeDirectory;
  env_["HADOOP_INSTALL"] = homeDirectory;
  env_["HADOOP_MAPRED_HOME"] = homeDirectory;
  env_["HADOOP_COMMON_HOME"] = homeDirectory;
  env_["HADOOP_HDFS_HOME"] = homeDirectory;
  env_["YARN_HOME"] = homeDirectory;
  env_["HADOOP_COMMON_LIB_NATIVE_DIR"] = homeDirectory + "/lib/native";
  env_["HADOOP_CONF_DIR"] = homeDirectory;
  env_["HADOOP_PREFIX"] = homeDirectory;
  env_["HADOOP_LIBEXEC_DIR"] = homeDirectory + "/libexec";
  env_["HADOOP_CONF_DIR"] = homeDirectory + "/etc/hadoop";
}
} // namespace bytedance::bolt::filesystems::test
