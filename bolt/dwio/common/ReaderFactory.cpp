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

#include "bolt/dwio/common/ReaderFactory.h"
namespace bytedance::bolt::dwio::common {

namespace {

using ReaderFactoriesMap =
    std::unordered_map<FileFormat, std::shared_ptr<ReaderFactory>>;

ReaderFactoriesMap& readerFactories() {
  static ReaderFactoriesMap factories;
  return factories;
}

} // namespace

bool registerReaderFactory(std::shared_ptr<ReaderFactory> factory) {
  bool ok = readerFactories().insert({factory->fileFormat(), factory}).second;
  // NOTE: re-enable this check after Prestissimo has updated dwrf registration.
#if 0
  BOLT_CHECK(
      ok,
      "ReaderFactory is already registered for format {}",
      toString(factory->fileFormat()));
#endif
  return true;
}

bool unregisterReaderFactory(FileFormat format) {
  auto count = readerFactories().erase(format);
  return count == 1;
}

std::shared_ptr<ReaderFactory> getReaderFactory(FileFormat format) {
  auto it = readerFactories().find(format);
  BOLT_CHECK(
      it != readerFactories().end(),
      "ReaderFactory is not registered for format {}",
      toString(format));
  return it->second;
}

} // namespace bytedance::bolt::dwio::common
