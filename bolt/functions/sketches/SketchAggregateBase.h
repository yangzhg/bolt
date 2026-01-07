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

#include "bolt/exec/Aggregate.h"
#include "bolt/vector/FlatVector.h"

#define BOLT_DYNAMIC_SCALAR_SKETCH_TYPE_DISPATCH(TEMPLATE_FUNC, typeKind, ...) \
  [&]() {                                                                      \
    switch (typeKind) {                                                        \
      case ::bytedance::bolt::TypeKind::BOOLEAN: {                             \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::BOOLEAN>(            \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::INTEGER: {                             \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::INTEGER>(            \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::TINYINT: {                             \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::TINYINT>(            \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::SMALLINT: {                            \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::SMALLINT>(           \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::BIGINT: {                              \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::BIGINT>(             \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::REAL: {                                \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::REAL>(__VA_ARGS__);  \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::DOUBLE: {                              \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::DOUBLE>(             \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::VARCHAR: {                             \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::VARCHAR>(            \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::VARBINARY: {                           \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::VARBINARY>(          \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::bytedance::bolt::TypeKind::TIMESTAMP: {                           \
        return TEMPLATE_FUNC<::bytedance::bolt::TypeKind::TIMESTAMP>(          \
            __VA_ARGS__);                                                      \
      }                                                                        \
      default:                                                                 \
        BOLT_FAIL("not a scalar type! kind: {}", mapTypeKindToName(typeKind)); \
    }                                                                          \
  }()
namespace bytedance::bolt::aggregate {
namespace {

template <typename T, typename TAccumulator>
class SketchAggregateBase : public exec::Aggregate {
 protected:
  DecodedVector decodedValue_;

 protected:
  TAccumulator* getAccumulator(char* group) {
    return value<TAccumulator>(group);
  }

 public:
  SketchAggregateBase(const TypePtr& resultType)
      : exec::Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override final {
    return sizeof(TAccumulator);
  }

  bool isFixedSize() const override final {
    return true;
  }

  bool accumulatorUsesExternalMemory() const override final {
    return true;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override final {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      auto group = groups[i];
      new (group + offset_) TAccumulator();
    }
  }

  void destroy(folly::Range<char**> groups) override final {
    for (auto group : groups) {
      auto* accumulator = getAccumulator(group);
      std::destroy_at(accumulator);
    }
  }

  virtual void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedValue_.decode(*args[0], rows, true);

    rows.applyToSelected([&](auto row) {
      if (decodedValue_.isNullAt(row)) {
        return;
      }
      auto group = groups[row];
      clearNull(group);

      StringView sv = decodedValue_.valueAt<StringView>(row);
      auto other = TAccumulator::deserialize(sv);
      auto accumulator = getAccumulator(group);
      accumulator->merge(other);
    });
  }

  virtual void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    auto accumulator = getAccumulator(group);
    decodedValue_.decode(*args[0], rows, true);

    rows.applyToSelected([&](auto row) {
      if (decodedValue_.isNullAt(row)) {
        return;
      }
      StringView sv = decodedValue_.valueAt<StringView>(row);
      auto other = TAccumulator::deserialize(sv);
      accumulator->merge(other);
    });
  }

  virtual void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    BOLT_DCHECK_LE(1, args.size());

    auto accumulator = getAccumulator(group);
    if (!accumulator->hasInitialized()) {
      accumulator->initialize(rows, args);
    }

    decodedValue_.decode(*args[0], rows, true);
    if (decodedValue_.mayHaveNulls()) {
      rows.applyToSelected([&](auto row) {
        if (decodedValue_.isNullAt(row)) {
          return;
        }

        accumulator->update(decodedValue_.valueAt<T>(row));
      });
    } else {
      rows.applyToSelected([&](auto row) {
        accumulator->update(decodedValue_.valueAt<T>(row));
      });
    }
  }

  virtual void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    decodedValue_.decode(*args[0], rows, true);

    if (decodedValue_.mayHaveNulls()) {
      rows.applyToSelected([&](auto row) {
        if (decodedValue_.isNullAt(row)) {
          return;
        }

        auto accumulator = getAccumulator(groups[row]);
        if (!accumulator->hasInitialized()) {
          accumulator->initialize(rows, args);
        }
        accumulator->update(decodedValue_.valueAt<T>(row));
      });
    } else {
      rows.applyToSelected([&](auto row) {
        auto accumulator = getAccumulator(groups[row]);
        if (!accumulator->hasInitialized()) {
          accumulator->initialize(rows, args);
        }
        accumulator->update(decodedValue_.valueAt<T>(row));
      });
    }
  }

  virtual void extractAccumulators(
      char** groups,
      int32_t numGroups,
      VectorPtr* result) override {
    BOLT_CHECK(result);
    auto flatResult = (*result)->asFlatVector<StringView>();
    flatResult->resize(numGroups);

    uint64_t* rawNulls = nullptr;
    if (flatResult->mayHaveNulls()) {
      BufferPtr nulls = flatResult->mutableNulls(flatResult->size());
      rawNulls = nulls->asMutable<uint64_t>();
    }

    for (auto i = 0; i < numGroups; ++i) {
      if (rawNulls) {
        bits::clearBit(rawNulls, i);
      }

      char* group = groups[i];
      auto accumulator = getAccumulator(group);
      auto serialized = accumulator->serialize();
      flatResult->set(i, StringView(serialized));
    }
  }
};

std::unordered_map<std::string, std::string> typeNameMap{
    {typeid(bool).name(), "b"},
    {typeid(int8_t).name(), "c"},
    {typeid(int16_t).name(), "s"},
    {typeid(int32_t).name(), "i"},
    {typeid(int64_t).name(), "l"},
    {typeid(float).name(), "f"},
    {typeid(double).name(), "d"},
    {typeid(StringView).name(), "sv"},
    {typeid(Timestamp).name(), "t"},
    {typeid(Date).name(), "dt"},
};

std::unordered_map<std::string, std::string> nameTypeMap{
    {"b", typeid(bool).name()},
    {"c", typeid(int8_t).name()},
    {"s", typeid(int16_t).name()},
    {"i", typeid(int32_t).name()},
    {"l", typeid(int64_t).name()},
    {"f", typeid(float).name()},
    {"d", typeid(double).name()},
    {"sv", typeid(StringView).name()},
    {"t", typeid(Timestamp).name()},
    {"dt", typeid(Date).name()},
};

std::vector<std::string> primitiveTypes{
    "boolean",
    "tinyint",
    "smallint",
    "integer",
    "bigint",
    "real",
    "double",
    "varchar",
    "timestamp",
    "date"};

} // namespace
} // namespace bytedance::bolt::aggregate
