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

#include <cstdint>
#include "bolt/common/base/Exceptions.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/Macros.h"
#include "bolt/functions/UDFOutputString.h"
#include "bolt/functions/lib/string/StringImpl.h"
namespace bytedance::bolt::functions {

// A helper class to append strings to the inner buffer of
// FlatVector<StingView> directly.
// It will allocate new buffers if the size of the latestest buffer
// is not big enough.
class InPlaceString {
 public:
  InPlaceString(FlatVector<StringView>* vector) {
    if (UNLIKELY(vector->stringBuffers().empty())) {
      allocateNewBuffer(vector);
      return;
    }
    buffer_ = vector->stringBuffers().back().get();
    if (UNLIKELY(!buffer_ || !buffer_->unique())) {
      allocateNewBuffer(vector);
    }
  }

  template <typename T>
  void append(T next, FlatVector<StringView>* vector) {
    if (UNLIKELY(next.size() > availableSize())) {
      setNewBuffer(size_ + next.size(), vector);
    }
    memcpy(data() + size_, next.data(), next.size());
    size_ += next.size();
  }

  void append(const char* str, int32_t size, FlatVector<StringView>* vector) {
    if (UNLIKELY(size > availableSize())) {
      setNewBuffer(size_ + size, vector);
    }
    memcpy(data() + size_, str, size);
    size_ += size;
  }

  void setNewBuffer(size_t new_size, FlatVector<StringView>* vector) {
    auto* oldBuffer = buffer_;
    allocateNewBuffer(vector, new_size);
    memcpy(data(), oldBuffer->as<char>() + oldBuffer->size(), size_);
  }

  void set(vector_size_t idx, FlatVector<StringView>* vector) {
    vector->setNoCopy(idx, StringView(data(), size_));
    if (size_ > StringView::kInlineSize) {
      buffer_->setSize(buffer_->size() + size_);
    }
  }

  uint64_t availableSize() const {
    return buffer_->capacity() - buffer_->size() - size_;
  }

  char* data() const {
    return buffer_->asMutable<char>() + buffer_->size();
  }

  uint64_t size() const {
    return size_;
  }

  void setSize(uint64_t size) {
    size_ = size;
  }

 private:
  void allocateNewBuffer(
      FlatVector<StringView>* vector,
      size_t newBufferSize = 0) {
    newBufferSize =
        std::max(newBufferSize, FlatVector<StringView>::kInitialStringSize);
    auto newBuffer =
        AlignedBuffer::allocate<char>(newBufferSize, vector->pool());
    BOLT_CHECK_NOT_NULL(newBuffer);
    newBuffer->setSize(0);
    vector->addStringBuffer(newBuffer);
    buffer_ = newBuffer.get();
    BOLT_CHECK_NOT_NULL(buffer_);
  }

  uint64_t size_{0};
  Buffer* buffer_;
};

std::vector<std::shared_ptr<exec::FunctionSignature>> ConcatWsSignatures();

std::shared_ptr<exec::VectorFunction> makeConcatWs(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config);

} // namespace bytedance::bolt::functions
