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

#include "bolt/connectors/hive/PaimonRowIterator.h"
namespace bytedance::bolt::connector::hive {

struct PaimonEngine {
  virtual vector_size_t add(PaimonRowIteratorPtr iterator) = 0;
  virtual vector_size_t finish() = 0;

  virtual void setResult(RowVectorPtr result_) {
    result = result_;
  }

  void append(VectorPtr output, PaimonRowIterator& iterator) {
    VLOG(2) << "Appending:"
            << "-->" << iterator.values->toString(iterator.rowIndex);
    vector_size_t targetIndex = output->size();
    output->resize(output->size() + 1);
    output->copy(iterator.values.get(), targetIndex, iterator.rowIndex, 1);
  }

  void remove(VectorPtr output, PaimonRowIterator& iterator) {
    output->resize(output->size() - 1);
  }

 protected:
  RowVectorPtr result;
};

} // namespace bytedance::bolt::connector::hive
