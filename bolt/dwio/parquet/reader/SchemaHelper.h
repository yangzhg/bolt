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
#include <unordered_map>
#include "bolt/dwio/parquet/thrift/codegen/parquet_types.h"
namespace bytedance::bolt::parquet {
using bytedance::bolt::parquet::thrift::SchemaElement;

class SchemaHelper {
 public:
  explicit SchemaHelper(const std::vector<SchemaElement>& schema);

  const SchemaElement& getElementByLeafIndex(size_t leafIndex) const;
  size_t mapLeafToSchemaIndex(size_t leafIndex) const;
  size_t getLeafCount() const;
  std::vector<std::string> getPath(size_t leafIndex) const;

 private:
  void buildMappings();

  const std::vector<SchemaElement>& schema_;
  std::unordered_map<size_t, size_t> leafToSchemaIndex_;
  std::unordered_map<size_t, std::vector<std::string>> leafToPaths_;
  size_t leafCount_{0};
};

} // namespace bytedance::bolt::parquet