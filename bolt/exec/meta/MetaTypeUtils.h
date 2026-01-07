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

#if defined(__GNUC__)
#define ALWAYS_INLINE inline __attribute__((__always_inline__))
#else
#define ALWAYS_INLINE inline
#endif

#include <bolt/type/Type.h>
namespace bytedance::bolt::exec {

/// @brief  A helper struct for template parameters deduction
///
/// @tparam ...KINDS
template <TypeKind... KINDS>
struct TypeKindHolder {
  static constexpr TypeKind typeKinds[] = {KINDS...};
  static constexpr std::size_t length{sizeof...(KINDS)};

  //   for debugging
  //   inline void hello(TestData *data) const {
  //     for (auto k : typeKinds) {
  //       std::cout << "kind: " << (int)k << "\t";
  //     }
  //     std::cout << "sizeof: " << sizeof...(KINDS) << "\t" << data <<
  //     std::endl;
  //   }
};

/// @brief  A generic declaration of TypeKindConcat
///
/// @tparam T
/// @tparam ...
template <typename T, typename...>
struct TypeKindConcat {
  using type = T;
};

/// @brief A template util to concat TEMPLATE parameters at compiling time.
///        TypeKindConcat<C<T1, T2>, C<T3, T4>> will produce  C<T1, T2, T3, T4>
///        Recursive Inheritance Idiom
///
///
/// @tparam ...Ts1
/// @tparam ...Ts2
/// @tparam ...Ts3
template <TypeKind... Ts1, TypeKind... Ts2, typename... Ts3>
struct TypeKindConcat<TypeKindHolder<Ts1...>, TypeKindHolder<Ts2...>, Ts3...>
    : public TypeKindConcat<TypeKindHolder<Ts1..., Ts2...>, Ts3...> {};

} // namespace bytedance::bolt::exec
