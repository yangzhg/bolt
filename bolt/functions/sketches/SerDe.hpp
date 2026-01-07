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
#include <cstring>

#include "DataSketches/memory_operations.hpp"
#include "DataSketches/serde.hpp"
#include "bolt/common/base/RandomUtil.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::aggregate {

template <typename T, typename Enable = void>
struct serde {
  void serialize(std::ostream& output, const T* items, unsigned nItems) const;
  void deserialize(std::istream& input, T* items, unsigned nItems)
      const; // items allocated but not initialized

  size_t size_of_item(const T& item) const;
  size_t serialize(void* ptr, size_t capacity, const T* items, unsigned nItems)
      const;
  size_t deserialize(
      const void* ptr,
      size_t capacity,
      T* items,
      unsigned nItems) const; // items allocated but not initialized
};

static void throwFailedWriteRuntimeError(unsigned nItems) {
  BOLT_FAIL(
      std::string("failed writing to std::ostream with ") +
      std::to_string(nItems) + " items");
}

static void throwFailedReadRuntimeError(unsigned nItems) {
  BOLT_FAIL(
      std::string("failed reading from std::istream with ") +
      std::to_string(nItems) + " items");
}

static void throwBadWriteRuntimeError(unsigned nItems) {
  BOLT_FAIL(
      std::string("error writing to std::ostream with ") +
      std::to_string(nItems) + " items");
}

static void throwBadReadRuntimeError(unsigned nItems) {
  BOLT_FAIL(
      std::string("error reading from std::istream with ") +
      std::to_string(nItems) + " items");
}

template <typename T>
struct serde<T> {
  void serialize(std::ostream& output, const T* items, unsigned nItems) const {
    try {
      output.write(reinterpret_cast<const char*>(items), sizeof(T) * nItems);
    } catch (std::ostream::failure&) {
      throwFailedWriteRuntimeError(nItems);
    }

    if (!output.good()) {
      throwBadWriteRuntimeError(nItems);
    }
  }

  void deserialize(std::istream& inputStream, T* items, unsigned nItems) const {
    try {
      inputStream.read((char*)items, sizeof(T) * nItems);
    } catch (std::istream::failure&) {
      throwFailedReadRuntimeError(nItems);
    }

    if (!inputStream.good()) {
      throwBadReadRuntimeError(nItems);
    }
  }

  size_t size_of_item(const T&) const {
    return sizeof(T);
  }
  size_t serialize(void* ptr, size_t capacity, const T* items, unsigned nItems)
      const {
    const size_t bytes_written = sizeof(T) * nItems;
    datasketches::check_memory_size(bytes_written, capacity);
    memcpy(ptr, items, bytes_written);
    return bytes_written;
  }
  size_t deserialize(
      const void* ptr,
      size_t capacity,
      T* items,
      unsigned nItems) const {
    const size_t bytes_read = sizeof(T) * nItems;
    datasketches::check_memory_size(bytes_read, capacity);
    memcpy(items, ptr, bytes_read);
    return bytes_read;
  }
};

template <>
struct serde<std::string> {
  void serialize(
      std::ostream& output,
      const std::string* items,
      unsigned nItems) const {
    unsigned i = 0;
    try {
      for (; i < nItems && output.good(); i++) {
        uint32_t length = static_cast<uint32_t>(items[i].size());
        output.write((char*)&length, sizeof(length));
        output.write(items[i].c_str(), length);
      }
    } catch (std::ostream::failure&) {
      throwFailedWriteRuntimeError(i);
    }
    if (!output.good()) {
      throwBadWriteRuntimeError(i);
    }
  }

  void deleteItems(std::string* items, int nItems) const {
    for (unsigned j = 0; j < nItems; ++j) {
      items[j].~basic_string();
    }
  }

  bool deserializeItem(std::istream& input, std::string* items, unsigned index)
      const {
    uint32_t length;
    input.read((char*)&length, sizeof(length));
    if (!input.good()) {
      return false;
    }
    std::string str;
    str.reserve(length);
    for (uint32_t j = 0; j < length; j++) {
      str.push_back(static_cast<char>(input.get()));
    }
    if (!input.good()) {
      return false;
    }
    new (&items[index]) std::string(std::move(str));
    return true;
  }

  void deserialize(std::istream& input, std::string* items, unsigned nItems)
      const {
    unsigned i = 0;
    try {
      for (; i < nItems; i++) {
        if (!deserializeItem(input, items, i))
          break;
      }
    } catch (std::istream::failure&) {
      throwFailedReadRuntimeError(i);
    }

    if (!input.good()) {
      deleteItems(items, i);
      throwBadReadRuntimeError(i);
    }
  }
  size_t size_of_item(const std::string& item) const {
    return sizeof(uint32_t) + item.size();
  }
  size_t serialize(
      void* ptr,
      size_t capacity,
      const std::string* items,
      unsigned nItems) const {
    size_t bytes_written = 0;
    for (unsigned i = 0; i < nItems; ++i) {
      const uint32_t length = static_cast<uint32_t>(items[i].size());
      const size_t new_bytes = length + sizeof(length);
      datasketches::check_memory_size(bytes_written + new_bytes, capacity);
      memcpy(ptr, &length, sizeof(length));
      ptr = static_cast<char*>(ptr) + sizeof(uint32_t);
      memcpy(ptr, items[i].c_str(), length);
      ptr = static_cast<char*>(ptr) + length;
      bytes_written += new_bytes;
    }
    return bytes_written;
  }

  bool deserializeItem(
      const void* ptr,
      size_t capacity,
      size_t& bytes_read,
      std::string* items,
      int index) const {
    uint32_t length = 0;
    if (bytes_read + sizeof(length) > capacity) {
      bytes_read += sizeof(length);
      return false;
    }
    memcpy(&length, ptr, sizeof(length));
    ptr = static_cast<const char*>(ptr) + sizeof(uint32_t);
    bytes_read += sizeof(length);

    if (bytes_read + length > capacity) {
      bytes_read += length; // we'll use this to report the error
      return false;
    }
    new (&items[index]) std::string(static_cast<const char*>(ptr), length);
    ptr = static_cast<const char*>(ptr) + length;
    bytes_read += length;
    return true;
  }

  size_t deserialize(
      const void* ptr,
      size_t capacity,
      std::string* items,
      unsigned nItems) const {
    size_t bytes_read = 0;

    for (unsigned i = 0; i < nItems; ++i) {
      if (!deserializeItem(ptr, capacity, bytes_read, items, i)) {
        deleteItems(items, i);

        // using this for a consistent error message
        datasketches::check_memory_size(bytes_read, capacity);
        break;
      }
    }

    return bytes_read;
  }
};

// TODO: add serialization implementation for below 3 items
template <typename T>
struct serde<
    T,
    typename std::enable_if<std::disjunction<
        typename std::is_same<T, DateType>::type,
        typename std::is_same<T, IntervalDayTimeType>::type,
        typename std::is_same<T, TimestampType>::type>::value>> {
  void serialize(std::ostream& output, const T* items, unsigned nItems) const {
    BOLT_NYI();
  }

  void deserialize(std::istream& input, T* items, unsigned nItems) const {
    BOLT_NYI();
  }

  size_t size_of_item(const T& item) const {
    BOLT_NYI();
  }
  size_t serialize(void* ptr, size_t capacity, const T* items, unsigned nItems)
      const {
    BOLT_NYI();
  }
  size_t deserialize(
      const void* ptr,
      size_t capacity,
      T* items,
      unsigned nItems) const {
    BOLT_NYI();
  }
};

} // namespace bytedance::bolt::aggregate
