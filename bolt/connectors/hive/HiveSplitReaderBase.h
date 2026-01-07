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

#include "bolt/connectors/hive/FileHandle.h"
#include "bolt/connectors/hive/HiveConnectorSplit.h"
#include "bolt/dwio/common/Options.h"
namespace bytedance::bolt {
class BaseVector;
class variant;
using VectorPtr = std::shared_ptr<BaseVector>;
} // namespace bytedance::bolt
namespace bytedance::bolt::dwio::common {
struct RuntimeStatistics;
} // namespace bytedance::bolt::dwio::common
namespace bytedance::bolt::connector::hive {

class HiveSplitReaderBase {
 public:
  virtual ~HiveSplitReaderBase();

  virtual uint64_t next(int64_t size, VectorPtr& output) = 0;

  virtual bool allPrefetchIssued() const = 0;

  virtual bool emptySplit() const = 0;

  virtual void resetFilterCaches() = 0;

  virtual int64_t estimatedRowSize() const = 0;

  virtual void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const = 0;

  virtual void resetSplit() = 0;
};

} // namespace bytedance::bolt::connector::hive
