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

#pragma once
namespace bytedance::bolt::functions::sparksql {

/// This class implements the array union function.
///
/// DEFINITION:
/// array_union(x, y) → array
/// Returns an array of the elements in the union of x and y, without
/// duplicates.
template <typename T>
struct ArrayUnionFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T)

  // Fast path for primitives.
  template <typename Out, typename In>
  void call(Out& out, const In& inputArray1, const In& inputArray2) {
    folly::F14FastSet<typename In::element_t> elementSet;
    bool nullAdded = false;
    bool nanAdded = false;
    auto addItems = [&](auto& inputArray) {
      for (const auto& item : inputArray) {
        if (item.has_value()) {
          if constexpr (
              std::is_same_v<In, arg_type<Array<float>>> ||
              std::is_same_v<In, arg_type<Array<double>>>) {
            bool isNaN = std::isnan(item.value());
            if ((isNaN && !nanAdded) ||
                (!isNaN && elementSet.insert(item.value()).second)) {
              auto& newItem = out.add_item();
              newItem = item.value();
            }
            if (!nanAdded && isNaN) {
              nanAdded = true;
            }
          } else if (elementSet.insert(item.value()).second) {
            auto& newItem = out.add_item();
            newItem = item.value();
          }
        } else if (!nullAdded) {
          nullAdded = true;
          out.add_null();
        }
      }
    };
    addItems(inputArray1);
    addItems(inputArray2);
  }

  void call(
      out_type<Array<Generic<T1>>>& out,
      const arg_type<Array<Generic<T1>>>& inputArray1,
      const arg_type<Array<Generic<T1>>>& inputArray2) {
    folly::F14FastSet<exec::GenericView> elementSet;
    bool nullAdded = false;
    auto addItems = [&](auto& inputArray) {
      for (const auto& item : inputArray) {
        if (item.has_value()) {
          if (elementSet.insert(item.value()).second) {
            auto& newItem = out.add_item();
            newItem.copy_from(item.value());
          }
        } else if (!nullAdded) {
          nullAdded = true;
          out.add_null();
        }
      }
    };
    addItems(inputArray1);
    addItems(inputArray2);
  }
};

/// This class implements the default.array_union udf.
///
/// DEFINITION:
/// default.array_union(x, y,...) → array
/// Returns an array of the elements in the union of n arrays, without
/// duplicates.
template <typename T>
struct ArrayUnionUdfFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T)

  // Fast path for primitives.
  template <typename Out, typename In>
  void callNullable(Out& out, const In* inputArrays) {
    BOLT_CHECK(inputArrays && inputArrays->size() >= 2);
    using InElementType = typename In::Element::element_t::element_t;
    std::set<InElementType> elementSet;
    bool nullAdded = false;
    bool nanAdded = false;
    InElementType nan = InElementType();
    auto addItems = [&](const auto& inputArray) {
      for (const auto& item : inputArray) {
        if (item.has_value()) {
          if constexpr (
              std::is_same_v<InElementType, float> ||
              std::is_same_v<InElementType, double>) {
            bool isNaN = std::isnan(item.value());
            if (!nanAdded && isNaN) {
              nanAdded = true;
              nan = item.value();
            } else if (!isNaN) {
              elementSet.insert(item.value());
            }
          } else {
            elementSet.insert(item.value());
          }
        } else if (!nullAdded) {
          nullAdded = true;
        }
      }
    };

    for (auto i = 0; i < inputArrays->size(); ++i) {
      auto const& input = (*inputArrays)[i];
      if (input.has_value()) {
        addItems(input.value());
      }
    }
    if (nullAdded) {
      out.add_null();
    }
    std::for_each(
        elementSet.cbegin(), elementSet.cend(), [&](InElementType element) {
          auto& newItem = out.add_item();
          newItem = element;
        });
    if (nanAdded) {
      auto& newItem = out.add_item();
      newItem = nan;
    }
  }
};
// todo : support Generic if needed

} // namespace bytedance::bolt::functions::sparksql
