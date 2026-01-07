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

#if !defined(BOLT_HAS_CUDF) || BOLT_HAS_CUDF != 1
#error "This header requires BOLT_HAS_CUDF to be defined and set to 1"
#endif

#include <fmt/format.h>
#include <folly/dynamic.h>
#include <sstream>

#include "bolt/common/base/Exceptions.h"

namespace bolt::cudf {

class CudfEnable {
 public:
  void setUseCudf() {
    useCudf_ = true;
  }

  bool useCudf() const {
    return useCudf_;
  }

  void addDetails(std::stringstream& stream) const {
    if (useCudf()) {
      stream << " " << kUseCudf;
    }
  }

  void serialize(folly::dynamic& obj) const {
    obj[kUseCudf] = useCudf();
  }

  void enableCudfIfDesired(const folly::dynamic& obj) {
    BOLT_CHECK(obj[kUseCudf].isBool());
    useCudf_ = obj[kUseCudf].asBool();
  }

 private:
  inline static constexpr std::string_view kUseCudf = "USE_CUDF";
  bool useCudf_ = false;
};

} // namespace bolt::cudf
