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

#include "bolt/dwio/txt/writer/RegisterTxtWriter.h"
#ifdef BOLT_ENABLE_TXT
#include "bolt/dwio/txt/writer/TxtWriter.h"
using bytedance::bolt::txt::writer::TxtWriterFactory;
#endif

namespace bytedance::bolt::txt {

void registerTxtWriterFactory() {
#ifdef BOLT_ENABLE_TXT
  dwio::common::registerWriterFactory(std::make_shared<TxtWriterFactory>());
#endif
}

void unregisterTxtWriterFactory() {
#ifdef BOLT_ENABLE_TXT
  dwio::common::unregisterWriterFactory(dwio::common::FileFormat::TEXT);
#endif
}

} // namespace bytedance::bolt::txt
