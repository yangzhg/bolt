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

#include "bolt/common/memory/MemoryUtils.h"

#include <unistd.h>
#include <fstream>
namespace bytedance::bolt::memory {

int64_t MemoryUtils::getProcessRss() {
  static const int64_t kPageSize = [] {
    const long pageSize = sysconf(_SC_PAGESIZE);
    return pageSize > 0 ? pageSize : 4096; // Typically 4096 bytes
  }();

  std::ifstream statmFile("/proc/self/statm");
  if (!statmFile) {
    return kInvalidRssSize;
  }

  int64_t rssPages = 0;
  // The first value is the total virtual memory size
  statmFile >> rssPages;
  // The second value is the number of pages in the RSS
  statmFile >> rssPages;
  return rssPages * kPageSize;
}

int64_t MemoryUtils::getOnHeapMemUsed() {
  if (hook) {
    return hook();
  }
  return kInvalidRssSize;
}

} // namespace bytedance::bolt::memory
