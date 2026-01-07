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

#include <boost/algorithm/string.hpp>
#include "bolt/expression/EvalCtx.h"
#include "bolt/expression/Expr.h"
#include "bolt/expression/StringWriter.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/expression/VectorWriters.h"
namespace bytedance::bolt::functions {

/**
 * str_to_long_arr(string) -> array(bigint)
 *           Splits string on delimiter and returns an array.
 */

class StrToLongArrFunction : public exec::VectorFunction {
 public:
  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;

    // varchar -> array(bigint)
    signatures.emplace_back(exec::FunctionSignatureBuilder()
                                .returnType("array(bigint)")
                                .argumentType("varchar")
                                .build());
    return signatures;
  }

 private:
  /**
   * The function calls the 'typed' template function based on the type of the
   * 'limit' argument.
   */
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // Get the decoded vectors out of arguments.
    exec::DecodedArgs decodedArgs(rows, args, context);
    DecodedVector* strings = decodedArgs.at(0);

    BaseVector::ensureWritable(rows, ARRAY(BIGINT()), context.pool(), result);
    exec::VectorWriter<Array<int64_t>> resultWriter;
    resultWriter.init(*result->as<ArrayVector>());
    rows.applyToSelected([&](vector_size_t row) {
      // Add new array (for the new row) to our array vector.
      resultWriter.setOffset(row);
      auto& arrayWriter = resultWriter.current();
      auto inputSv = strings->valueAt<StringView>(row);
      std::string_view input = std::string_view(inputSv.data(), inputSv.size());
      if (input.size() == 0) {
        resultWriter.commitNull();
      } else {
        int64_t currentNumber = 0;
        bool isNegative = false;
        bool isNumber = false;
        bool invalidNumber = false;
        for (char ch : input) {
          if (ch == ',') {
            if (isNumber && !invalidNumber) {
              arrayWriter.add_item() =
                  isNegative ? -currentNumber : currentNumber;
            }
            currentNumber = 0;
            isNegative = false;
            isNumber = false;
            invalidNumber = false;
          } else if (std::isdigit(ch)) {
            if (!invalidNumber) {
              isNumber = true;
              int digit = ch - '0';
              if (isNegative) {
                if (-currentNumber < INT64_MIN / 10 ||
                    (-currentNumber == INT64_MIN / 10 &&
                     digit > -(INT64_MIN % 10))) {
                  invalidNumber = true;
                  continue;
                }
              } else {
                if (currentNumber > INT64_MAX / 10 ||
                    (currentNumber == INT64_MAX / 10 &&
                     digit > INT64_MAX % 10)) {
                  invalidNumber = true;
                  continue;
                }
              }
              currentNumber = currentNumber * 10 + digit;
            }
          } else if ((ch == '-' || ch == '+') && !isNumber) {
            if (ch == '-') {
              isNegative = true;
            }
            isNumber = true;
          } else {
            invalidNumber = true;
          }
        }
        if (isNumber && !invalidNumber) {
          arrayWriter.add_item() = isNegative ? -currentNumber : currentNumber;
        }
        resultWriter.commit();
      }
    });
    resultWriter.finish();

    // Ensure that our result elements vector uses the same string buffer as
    // the input vector of strings.
    result->as<ArrayVector>()->elements()->as<FlatVector<int64_t>>();
  }
};

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_str_to_long_arr,
    StrToLongArrFunction::signatures(),
    std::make_unique<StrToLongArrFunction>());

} // namespace bytedance::bolt::functions
