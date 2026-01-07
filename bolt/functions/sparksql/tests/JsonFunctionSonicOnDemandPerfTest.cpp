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

#include <gtest/gtest.h>
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "bolt/common/base/Fs.h"
#include "bolt/dwio/common/tests/utils/DataFiles.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"
namespace bytedance::bolt::functions::sparksql::test {
using namespace bytedance::bolt::test;

namespace {
class JsonFunctionSonicOnDemandPerfTest : public SparkFunctionBaseTest {
 protected:
  void testJsonExtract(
      const std::vector<std::optional<std::string>>& input,
      const std::string& json_path,
      const std::vector<std::optional<std::string>>& output);

  std::optional<std::string> testGetJsonObjectOnce(
      const std::optional<std::string>& input,
      const std::optional<std::string>& json_path);

  const int perf_iterations_ = 100;
  // const int perf_iterations_ = 1;
  static void readLargeJsonIntoString(std::string& json) {
    std::string large_json = getDataFilePath("data/large_slow_parse.json");

    std::ifstream file(large_json); // Open the file

    // Check if the file was successfully opened
    EXPECT_TRUE(file);

    // Use stringstream to read the entire file into a string
    std::stringstream buffer;
    buffer << file.rdbuf(); // Read the entire file content into the buffer

    // Get the content from the buffer into a string
    json = buffer.str();
  }
};

void JsonFunctionSonicOnDemandPerfTest::testJsonExtract(
    const std::vector<std::optional<std::string>>& input,
    const std::string& json_path,
    const std::vector<std::optional<std::string>>& output) {}

std::optional<std::string>
JsonFunctionSonicOnDemandPerfTest::testGetJsonObjectOnce(
    const std::optional<std::string>& input,
    const std::optional<std::string>& json_path) {
  return evaluateOnce<std::string>("get_json_object(c0, c1)", input, json_path);
}

TEST_F(JsonFunctionSonicOnDemandPerfTest, slowParseLargeJsonSimdJson) {
  auto start1 = std::chrono::high_resolution_clock::now();

  for (auto i = 0; i < this->perf_iterations_; i++) {
    for (auto value : {"true"}) {
      queryCtx_->testingOverrideConfigUnsafe(
          {{core::QueryConfig::kUseDOMParserInGetJsonObject, "false"},
           {core::QueryConfig::kGetJsonObjectEscapeEmoji, "false"},
           {core::QueryConfig::kUseSonicJson, "false"}});
      {
        std::string json = {};
        readLargeJsonIntoString(json);
        testGetJsonObjectOnce(json, "$.module");
        testGetJsonObjectOnce(json, "$.id_info");
        testGetJsonObjectOnce(json, "$.data_type");
      }
    }
  }
  auto end1 = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end1 - start1);
  std::cerr << "simdjson time: " << duration.count() << " ms\n";
}

TEST_F(JsonFunctionSonicOnDemandPerfTest, slowParseLargeJsonOnDemand) {
  auto start1 = std::chrono::high_resolution_clock::now();

  for (auto i = 0; i < this->perf_iterations_; i++) {
    for (auto value : {"true"}) {
      queryCtx_->testingOverrideConfigUnsafe(
          {{core::QueryConfig::kUseDOMParserInGetJsonObject, "false"},
           {core::QueryConfig::kGetJsonObjectEscapeEmoji, "false"},
           {core::QueryConfig::kUseSonicJson, "true"}});
      {
        std::string json = {};
        readLargeJsonIntoString(json);
        testGetJsonObjectOnce(json, "$.module");
        testGetJsonObjectOnce(json, "$.id_info");
        testGetJsonObjectOnce(json, "$.data_type");
      }
    }
  }
  auto end1 = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end1 - start1);
  std::cerr << "sonic ondemand: " << duration.count() << " ms\n";
}
} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
