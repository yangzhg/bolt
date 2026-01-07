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

#include "bolt/dwio/common/tests/utils/DataFiles.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/Fs.h"
namespace bytedance::bolt::test {

std::string getDataFilePath(const std::string& filePath) {
  std::string current_path = fs::current_path().c_str();
  return getDataFilePath(fs::current_path().c_str(), filePath);
}

std::string getDataFilePath(
    const std::string& baseDir,
    const std::string& filePath) {
  std::filesystem::path current = fs::current_path();
  std::filesystem::path base = baseDir;
  std::filesystem::path file = filePath;
  auto ret = std::filesystem::absolute(current / base / file);
  if (!fs::exists(ret)) {
    BOLT_CHECK(
        fs::exists(ret),
        "File not found: {}, current path: {}",
        ret.c_str(),
        current.c_str());
  }
  return ret.c_str();
}

} // namespace bytedance::bolt::test
