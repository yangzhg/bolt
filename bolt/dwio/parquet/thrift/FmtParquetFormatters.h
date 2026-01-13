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
#include <fmt/format.h>
#include <fmt/ostream.h>

#include "bolt/dwio/parquet/thrift/codegen/parquet_types.h"

template <>
struct fmt::formatter<bytedance::bolt::parquet::thrift::Type::type>
    : fmt::ostream_formatter {};

template <>
struct fmt::formatter<bytedance::bolt::parquet::thrift::ConvertedType::type>
    : fmt::ostream_formatter {};

template <>
struct fmt::formatter<
    bytedance::bolt::parquet::thrift::FieldRepetitionType::type>
    : fmt::ostream_formatter {};

template <>
struct fmt::formatter<bytedance::bolt::parquet::thrift::CompressionCodec::type>
    : fmt::ostream_formatter {};

template <>
struct fmt::formatter<bytedance::bolt::parquet::thrift::Encoding::type>
    : fmt::ostream_formatter {};
