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

#include "bolt/dwio/parquet/thrift/codegen/parquet_types.h"
namespace bytedance::bolt::parquet {

/**
 * Checks whether a column is fully dictionary-encoded.
 * Also maps the logical type (logicalType/converted_type) to the
 * "effective" Parquet physical type for further dictionary decoding.
 */
class DictionaryEncodingInfo {
 public:
  DictionaryEncodingInfo(
      const thrift::ColumnMetaData& meta,
      const thrift::SchemaElement& schema)
      : meta_(meta), schema_(schema) {
    analyze();
  }

  bool isFullyDictionaryEncoded() const {
    return isFullyDictionaryEncoded_;
  }

  /** The physical type we will decode (e.g., INT32, BYTE_ARRAY) */
  thrift::Type::type effectiveType() const {
    return effectiveType_;
  }

 private:
  void analyze() {
    analyzeEncodings();
    determineEffectiveType();
  }

  void analyzeEncodings() {
    // If encoding_stats is present, that’s the most accurate.
    if (meta_.__isset.encoding_stats) {
      int32_t totalDataPages = 0;
      int32_t dictDataPages = 0;

      for (auto& es : meta_.encoding_stats) {
        if (es.page_type == thrift::PageType::DATA_PAGE ||
            es.page_type == thrift::PageType::DATA_PAGE_V2) {
          totalDataPages += es.count;
          if (es.encoding == thrift::Encoding::RLE_DICTIONARY ||
              es.encoding == thrift::Encoding::PLAIN_DICTIONARY) {
            dictDataPages += es.count;
          }
        }
      }
      isFullyDictionaryEncoded_ =
          (totalDataPages > 0 && totalDataPages == dictDataPages);
    } else {
      // Fallback: check encodings list
      for (auto e : meta_.encodings) {
        switch (e) {
          case thrift::Encoding::PLAIN_DICTIONARY:
          case thrift::Encoding::RLE_DICTIONARY:
          case thrift::Encoding::RLE: // used for def/rep levels
          case thrift::Encoding::BIT_PACKED: // used for def/rep levels
            // possibly dictionary-encoded
            break;
          default:
            isFullyDictionaryEncoded_ = false;
            return;
        }
      }
      // if we didn’t return, everything is dictionary-compatible
      isFullyDictionaryEncoded_ = true;
    }
  }

  void determineEffectiveType() {
    effectiveType_ = meta_.type;

    if (schema_.__isset.logicalType) {
      const auto& lt = schema_.logicalType;
      if (lt.__isset.STRING) {
        effectiveType_ = thrift::Type::BYTE_ARRAY;
      } else if (lt.__isset.INTEGER) {
        auto bw = lt.INTEGER.bitWidth;
        effectiveType_ = (bw <= 32) ? thrift::Type::INT32 : thrift::Type::INT64;
      } else if (lt.__isset.DECIMAL) {
        // For DECIMAL we use FIXED_LEN_BYTE_ARRAY
        effectiveType_ = thrift::Type::FIXED_LEN_BYTE_ARRAY;
      } else if (lt.__isset.DATE) {
        effectiveType_ = thrift::Type::INT32; // Dates are stored as INT32
      } else if (lt.__isset.TIMESTAMP) {
        effectiveType_ = thrift::Type::INT64; // Timestamps as INT64
      }
    } else if (schema_.__isset.converted_type) {
      switch (schema_.converted_type) {
        case thrift::ConvertedType::UTF8:
          effectiveType_ = thrift::Type::BYTE_ARRAY;
          break;
        case thrift::ConvertedType::INT_8:
        case thrift::ConvertedType::INT_16:
        case thrift::ConvertedType::INT_32:
        case thrift::ConvertedType::DATE:
          effectiveType_ = thrift::Type::INT32;
          break;
        case thrift::ConvertedType::INT_64:
        case thrift::ConvertedType::TIMESTAMP_MILLIS:
        case thrift::ConvertedType::TIMESTAMP_MICROS:
          effectiveType_ = thrift::Type::INT64;
          break;
        case thrift::ConvertedType::DECIMAL:
          // For DECIMAL, check the precision to determine type
          if (schema_.__isset.precision) {
            if (schema_.precision <= 9) {
              effectiveType_ = thrift::Type::INT32;
            } else if (schema_.precision <= 18) {
              effectiveType_ = thrift::Type::INT64;
            } else {
              effectiveType_ = thrift::Type::FIXED_LEN_BYTE_ARRAY;
            }
          }
          break;
        default:
          break;
      }
    }
  }

  const thrift::ColumnMetaData& meta_;
  const thrift::SchemaElement& schema_;
  bool isFullyDictionaryEncoded_{false};
  thrift::Type::type effectiveType_;
};

} // namespace bytedance::bolt::parquet
