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

#include "bolt/expression/VectorFunction.h"
#include "bolt/expression/VectorWriters.h"
#include "bolt/functions/lib/string/JavaStyleSplit.h"
#include "bolt/functions/lib/string/RegexUtils.h"
namespace bytedance::bolt::functions {
namespace {
enum MapKeyDedupPolicy { EXCEPTION, LAST_WIN, FIRST_WIN };

// Creates a map after splitting the text into key/value pairs using delimiters.
// Default delimiters are ',' for `pairDelim` and ':' for `keyValueDelim`. Both
// `pairDelim` and `keyValueDelim` are treated as regular expressions."
template <
    MapKeyDedupPolicy Policy,
    bool PairDelimReg,
    bool KeyValueDelimReg,
    bool IsFlinkCompatible>
class StringToMap final : public exec::VectorFunction {
 public:
  explicit StringToMap(
      const std::string& pairDelim,
      const std::string& keyValueDelim)
      : pairDelimStr_(pairDelim), keyValueDelimStr_(keyValueDelim) {
    pairDelimSize_ = pairDelimStr_.size();
    keyValueDelimSize_ = keyValueDelimStr_.size();
    pairDelim_ = pairDelimStr_;
    keyValueDelim_ = keyValueDelimStr_;
    if constexpr (PairDelimReg) {
      pairDelimRegex_ = regex::compileRegexPattern(pairDelim);
    }
    if constexpr (KeyValueDelimReg) {
      keyValueDelimRegex_ = regex::compileRegexPattern(keyValueDelim);
    }

    BOLT_USER_CHECK(
        pairDelimSize_ >= 1,
        "pairDelimiter's size should be greater than or equal to 1, actual is {}.",
        pairDelimSize_);
    BOLT_USER_CHECK(
        keyValueDelimSize_ >= 1,
        "keyValueDelimiter's size should be greater than or equal to 1, actual is {}.",
        keyValueDelimSize_);
  }

  bool isDefaultNullBehavior() const override {
    return false;
  }
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    BaseVector::ensureWritable(
        rows, MAP(VARCHAR(), VARCHAR()), context.pool(), result);
    exec::LocalDecodedVector input(context, *args[0], rows);
    exec::VectorWriter<Map<Varchar, Varchar>> resultWriter;
    resultWriter.init(*result->as<MapVector>());
    rows.applyToSelected([&](vector_size_t row) {
      resultWriter.setOffset(row);
      if (input->isNullAt(row)) {
        resultWriter.commitNull();
        return;
      }
      auto& mapWriter = resultWriter.current();
      auto sv = input->valueAt<StringView>(row);
      const std::string_view current = std::string_view(sv.data(), sv.size());
      size_t start = 0;
      size_t end = 0;
      // used in Policy == LAST_WIN
      folly::F14FastMap<std::string_view, std::string_view> keyValues;
      folly::F14FastSet<std::string_view> nulls;
      folly::F14FastSet<std::string_view> keys;
      auto processKeyValue = [&](std::string_view key, std::string_view value) {
        if constexpr (Policy == EXCEPTION) {
          BOLT_USER_CHECK(
              keys.insert(key).second,
              "Duplicate map key {} was found, please check the input data. If you want "
              "to remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to "
              "LAST_WIN so that the key inserted at last takes precedence.",
              key);
          auto [keyWriter, valueWriter] = mapWriter.add_item();
          keyWriter.setNoCopy(StringView(key));
          valueWriter.setNoCopy(StringView(value));
        } else if constexpr (Policy == FIRST_WIN) {
          if (keys.insert(key).second) {
            auto [keyWriter, valueWriter] = mapWriter.add_item();
            keyWriter.setNoCopy(StringView(key));
            valueWriter.setNoCopy(StringView(value));
          }
        } else if constexpr (Policy == LAST_WIN) {
          nulls.erase(key);
          keyValues.insert_or_assign(key, value);
        } else {
          BOLT_UNREACHABLE();
        }
      };
      auto processKeyValueWithNull = [&](std::string_view entry) {
        if constexpr (IsFlinkCompatible) {
          if (entry.empty()) {
            return;
          }
        }
        if constexpr (Policy == EXCEPTION) {
          BOLT_USER_CHECK(
              keys.insert(entry).second,
              "Duplicate map key {} was found, please check the input data. If you want "
              "to remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to "
              "LAST_WIN so that the key inserted at last takes precedence.",
              entry);
          mapWriter.add_null().setNoCopy(StringView(entry));
        } else if constexpr (Policy == FIRST_WIN) {
          if (keys.insert(entry).second) {
            mapWriter.add_null().setNoCopy(StringView(entry));
          }
        } else if constexpr (Policy == LAST_WIN) {
          keyValues.erase(entry);
          nulls.insert(entry);
        } else {
          BOLT_UNREACHABLE();
        }
      };
      auto processEntry = [&](std::string_view entry) {
        if constexpr (KeyValueDelimReg) {
          auto res =
              javaStyle::javaStyleSplitRegex(entry, keyValueDelimRegex_, 2);
          if (res.size() == 2) {
            processKeyValue(res[0], res[1]);
          } else {
            processKeyValueWithNull(entry);
          }
        } else {
          size_t separatorPos = entry.find(keyValueDelim_);
          if (separatorPos != std::string_view::npos) {
            std::string_view key = entry.substr(0, separatorPos);
            std::string_view value =
                entry.substr(separatorPos + keyValueDelimSize_);
            processKeyValue(key, value);
          } else {
            processKeyValueWithNull(entry);
          }
        }
      };
      if constexpr (PairDelimReg) {
        auto res = javaStyle::javaStyleSplitRegex(current, pairDelimRegex_, -1);
        for (auto entry : res) {
          processEntry(entry);
        }
      } else {
        do {
          end = current.find(pairDelim_, start);
          std::string_view entry = current.substr(start, end - start);
          processEntry(entry);
          start = end + pairDelimSize_;
        } while (end != std::string_view::npos);
      }

      if constexpr (Policy == LAST_WIN) {
        for (const auto& [key, value] : keyValues) {
          auto [keyWriter, valueWriter] = mapWriter.add_item();
          keyWriter.setNoCopy(StringView(key));
          valueWriter.setNoCopy(StringView(value));
        }
        for (auto& entry : nulls) {
          mapWriter.add_null().setNoCopy(StringView(entry));
        }
      }
      resultWriter.commit();
    });
    resultWriter.finish();
    result->as<MapVector>()
        ->mapKeys()
        ->as<FlatVector<StringView>>()
        ->acquireSharedStringBuffers(args[0].get());
    result->as<MapVector>()
        ->mapValues()
        ->as<FlatVector<StringView>>()
        ->acquireSharedStringBuffers(args[0].get());
  }

 private:
  std::string pairDelimStr_;
  std::string keyValueDelimStr_;

  std::string_view pairDelim_;
  std::string_view keyValueDelim_;
  size_t pairDelimSize_;
  size_t keyValueDelimSize_;
  icu::LocalPointer<icu::RegexPattern> pairDelimRegex_;
  icu::LocalPointer<icu::RegexPattern> keyValueDelimRegex_;
};

template <bool PairDelimReg, bool KeyValueDelimReg>
std::shared_ptr<exec::VectorFunction> transmit(
    const std::string& pairDelim,
    const std::string& keyValueDelim,
    const std::string& mapKeyDedupPolicy,
    bool isFlinkCompatible) {
  if (mapKeyDedupPolicy == "EXCEPTION") {
    if (isFlinkCompatible) {
      return std::make_shared<
          StringToMap<EXCEPTION, PairDelimReg, KeyValueDelimReg, true>>(
          pairDelim, keyValueDelim);
    } else {
      return std::make_shared<
          StringToMap<EXCEPTION, PairDelimReg, KeyValueDelimReg, false>>(
          pairDelim, keyValueDelim);
    }
  } else if (mapKeyDedupPolicy == "LAST_WIN") {
    if (isFlinkCompatible) {
      return std::make_shared<
          StringToMap<LAST_WIN, PairDelimReg, KeyValueDelimReg, true>>(
          pairDelim, keyValueDelim);
    } else {
      return std::make_shared<
          StringToMap<LAST_WIN, PairDelimReg, KeyValueDelimReg, false>>(
          pairDelim, keyValueDelim);
    }
  } else if (mapKeyDedupPolicy == "FIRST_WIN") {
    if (isFlinkCompatible) {
      return std::make_shared<
          StringToMap<FIRST_WIN, PairDelimReg, KeyValueDelimReg, true>>(
          pairDelim, keyValueDelim);
    } else {
      return std::make_shared<
          StringToMap<FIRST_WIN, PairDelimReg, KeyValueDelimReg, false>>(
          pairDelim, keyValueDelim);
    }
  } else {
    BOLT_FAIL("Unknown mapKeyDedupPolicy: {}", mapKeyDedupPolicy);
  }
}

std::shared_ptr<exec::VectorFunction> createStringToMap(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  // according to
  // https://docs.databricks.com/en/sql/language-manual/functions/str_to_map.html
  // pairDelim and keyValueDelim must be STRING literal
  std::string pairDelim = ",";
  std::string keyValueDelim = ":";
  bool isPairDelimReg = false;
  bool isKeyValueDelimReg = false;
  if (inputArgs.size() >= 2) {
    BaseVector* constantPairDelim = inputArgs[1].constantValue.get();
    if (constantPairDelim) {
      pairDelim = std::string(
          constantPairDelim->as<ConstantVector<StringView>>()->valueAt(0));
      isPairDelimReg = stringCore::checkRegexPattern(pairDelim);
    } else {
      BOLT_USER_FAIL("pairDelim in str_to_map must be STRING literal.");
    }
  }
  if (inputArgs.size() == 3) {
    BaseVector* constantKeyValueDelim = inputArgs[2].constantValue.get();
    if (constantKeyValueDelim) {
      keyValueDelim = std::string(
          constantKeyValueDelim->as<ConstantVector<StringView>>()->valueAt(0));
      isKeyValueDelimReg = stringCore::checkRegexPattern(keyValueDelim);
    } else {
      BOLT_USER_FAIL("keyValueDelim in str_to_map must be STRING literal.");
    }
  }
  std::string mapKeyDedupPolicy = config.sparkMapKeyDedupPolicy();
  bool isFlinkCompatible = config.enableFlinkCompatible();

  if (isPairDelimReg) {
    if (isKeyValueDelimReg) {
      return transmit<true, true>(
          pairDelim, keyValueDelim, mapKeyDedupPolicy, isFlinkCompatible);
    } else {
      return transmit<true, false>(
          pairDelim, keyValueDelim, mapKeyDedupPolicy, isFlinkCompatible);
    }
  } else {
    if (isKeyValueDelimReg) {
      return transmit<false, true>(
          pairDelim, keyValueDelim, mapKeyDedupPolicy, isFlinkCompatible);
    } else {
      return transmit<false, false>(
          pairDelim, keyValueDelim, mapKeyDedupPolicy, isFlinkCompatible);
    }
  }
}

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  // varchar, varchar, varchar -> map(varchar, varchar)
  return {exec::FunctionSignatureBuilder()
              .returnType("map(varchar,varchar)")
              .argumentType("varchar")
              .constantArgumentType("varchar")
              .constantArgumentType("varchar")
              .build()};
}
} // namespace

BOLT_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_str_to_map,
    signatures(),
    createStringToMap);

} // namespace bytedance::bolt::functions
