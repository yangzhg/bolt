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

#include "bolt/dwio/common/WriterFactory.h"
namespace bytedance::bolt::dwio::common {

namespace {

using WriterFactoriesMap =
    std::unordered_map<FileFormat, std::shared_ptr<WriterFactory>>;

WriterFactoriesMap& writerFactories() {
  static WriterFactoriesMap factories;
  return factories;
}

} // namespace

bool registerWriterFactory(std::shared_ptr<WriterFactory> factory) {
  const bool ok =
      writerFactories().insert({factory->fileFormat(), factory}).second;
// TODO: enable the check after Prestissimo adds to register the dwrf writer.
#if 0
  BOLT_CHECK(
      ok,
      "WriterFactory is already registered for format {}",
      toString(factory->fileFormat()));
#endif
  return true;
}

bool unregisterWriterFactory(FileFormat format) {
  return (writerFactories().erase(format) == 1);
}

std::shared_ptr<WriterFactory> getWriterFactory(FileFormat format) {
  auto it = writerFactories().find(format);
  BOLT_CHECK(
      it != writerFactories().end(),
      "WriterFactory is not registered for format {}",
      toString(format));
  return it->second;
}

bool hasWriterFactory(FileFormat format) {
  return (writerFactories().count(format) == 1);
}

} // namespace bytedance::bolt::dwio::common
