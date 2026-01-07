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

#include "bolt/dwio/txt/reader/RegisterTxtReader.h"
#ifdef BOLT_ENABLE_TXT
#include "bolt/dwio/txt/reader/TxtReader.h"
using bytedance::bolt::txt::reader::TxtReaderFactory;
#endif

namespace bytedance::bolt::txt {
void registerTxtReaderFactory() {
#ifdef BOLT_ENABLE_TXT
  dwio::common::registerReaderFactory(std::make_shared<TxtReaderFactory>());
#endif
}

void unregisterTxtReaderFactory() {
#ifdef BOLT_ENABLE_TXT
  dwio::common::unregisterReaderFactory(dwio::common::FileFormat::TEXT);
#endif
}

} // namespace bytedance::bolt::txt
