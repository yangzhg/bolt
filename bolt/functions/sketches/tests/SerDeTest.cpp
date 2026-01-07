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

#include <functions/sketches/SerDe.hpp>
#include <gtest/gtest.h>
namespace bytedance::bolt::aggregate::sketches::test {

class FailBitOutputStream : private std::streambuf, public std::ostream {
 public:
  FailBitOutputStream() : std::ostream(this) {}

  std::ostream& write(const char* s, std::streamsize n) {
    this->setstate(std::ios::failbit);
    return *this;
  }
};

class FailBitInputStream : private std::streambuf, public std::istream {
 public:
  FailBitInputStream() : std::istream(this) {}

  std::istream& read(const char* s, std::streamsize n) {
    this->setstate(std::ios::failbit);
    return *this;
  }
};

class ThrowingOutputStream : private std::streambuf, public std::ostream {
 public:
  ThrowingOutputStream() : std::ostream(this) {}

  std::ostream& write(const char* s, std::streamsize n) {
    throw std::ostream::failure("failed");
    return *this;
  }
};

class ThrowingInputStream : private std::streambuf, public std::istream {
 public:
  ThrowingInputStream() : std::istream(this) {}

  std::istream& read(const char* s, std::streamsize n) {
    throw std::ostream::failure("failed");
    return *this;
  }
};

class SerDeTest : public testing::Test {};

TEST_F(SerDeTest, failWrite) {
  serde<int> serializer;
  FailBitOutputStream output;
  int item;
  try {
    serializer.serialize(output, &item, sizeof(item));
    FAIL();
  } catch (BoltRuntimeError const& e) {
    output.clear(std::ios::failbit);
    ASSERT_EQ(e.message(), "error writing to std::ostream with 4 items");
  }
}

TEST_F(SerDeTest, failRead) {
  serde<int> deserializer;
  FailBitInputStream output;
  int item;
  try {
    deserializer.deserialize(output, &item, sizeof(item));
    FAIL();
  } catch (BoltRuntimeError const& e) {
    output.clear(std::ios::failbit);
    ASSERT_EQ(e.message(), "error reading from std::istream with 4 items");
  }
}

TEST_F(SerDeTest, throwWrite) {
  serde<int> serializer;
  ThrowingOutputStream output;
  int item;
  try {
    serializer.serialize(output, &item, sizeof(item));
    FAIL();
  } catch (BoltRuntimeError const& e) {
    ASSERT_EQ(e.message(), "error writing to std::ostream with 4 items");
  }
}

TEST_F(SerDeTest, throwRead) {
  serde<int> deserializer;
  ThrowingInputStream output;
  int item;
  try {
    deserializer.deserialize(output, &item, sizeof(item));
    FAIL();
  } catch (BoltRuntimeError const& e) {
    ASSERT_EQ(e.message(), "error reading from std::istream with 4 items");
  }
}
} // namespace bytedance::bolt::aggregate::sketches::test
