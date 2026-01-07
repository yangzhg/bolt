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

#include "bolt/connectors/hive/FileHandle.h"
#include "bolt/common/base/Counters.h"
#include "bolt/common/base/StatsReporter.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/time/Timer.h"

#include <atomic>
namespace bytedance::bolt {

namespace {
// The group tracking is at the level of the directory, i.e. Hive partition.
std::string groupName(const std::string& filename) {
  const char* slash = strrchr(filename.c_str(), '/');
  return slash ? std::string(filename.data(), slash - filename.data())
               : filename;
}
} // namespace

std::shared_ptr<FileHandle> FileHandleGenerator::operator()(
    const std::string& filename,
    const filesystems::FileOptions& fileOptions) {
  // We have seen cases where drivers are stuck when creating file handles.
  // Adding a trace here to spot this more easily in future.
  process::TraceContext trace("FileHandleGenerator::operator()");
  uint64_t elapsedTimeUs{0};
  std::shared_ptr<FileHandle> fileHandle;
  {
    MicrosecondTimer timer(&elapsedTimeUs);
    fileHandle = std::make_shared<FileHandle>();
    fileHandle->file = filesystems::getFileSystem(filename, properties_)
                           ->openFileForRead(filename, fileOptions);
    fileHandle->uuid = StringIdLease(fileIds(), filename);
    fileHandle->groupId = StringIdLease(fileIds(), groupName(filename));
    VLOG(1) << "Generating file handle for: " << filename
            << " uuid: " << fileHandle->uuid.id();
  }
  RECORD_HISTOGRAM_METRIC_VALUE(
      kMetricHiveFileHandleGenerateLatencyMs, elapsedTimeUs / 1000);
  // TODO: build the hash map/etc per file type -- presumably after reading
  // the appropriate magic number from the file, or perhaps we include the file
  // type in the file handle key.
  return fileHandle;
}

} // namespace bytedance::bolt
