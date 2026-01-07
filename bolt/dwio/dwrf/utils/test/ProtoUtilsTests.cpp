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

#include <gtest/gtest.h>
#include "bolt/dwio/dwrf/utils/ProtoUtils.h"
#include "bolt/type/fbhive/HiveTypeParser.h"
#include "bolt/type/fbhive/HiveTypeSerializer.h"
using namespace bytedance::bolt::dwrf;
using namespace bytedance::bolt::type::fbhive;

TEST(ProtoUtilsTests, AllTypes) {
  std::vector<std::string> types{
      "struct<a:boolean,b:tinyint,c:smallint,d:int,e:bigint,f:float,g:double,f:string,g:binary,h:timestamp>",
      "struct<a:map<int,array<struct<a:map<string,int>,b:array<int>>>>>"};

  for (auto& type : types) {
    HiveTypeParser parser;
    auto schema = parser.parse(type);
    proto::Footer footer;
    ProtoUtils::writeType(*schema, footer);

    auto out = ProtoUtils::fromFooter(footer);
    auto str = HiveTypeSerializer::serialize(out);

    EXPECT_EQ(str, type);
  }
}

TEST(ProtoUtilsTests, Projection) {
  HiveTypeParser parser;
  auto schema = parser.parse(
      "struct<a:boolean,b:tinyint,c:smallint,d:struct<a:int,b:int,c:int>>");
  proto::Footer footer;
  ProtoUtils::writeType(*schema, footer);

  auto type = ProtoUtils::fromFooter(
      footer, [](auto id) { return id != 2 && id != 5; });
  auto res = HiveTypeSerializer::serialize(type);

  EXPECT_EQ("struct<a:boolean,c:smallint,d:struct<b:int,c:int>>", res);
}
