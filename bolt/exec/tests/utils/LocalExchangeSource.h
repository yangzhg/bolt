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
#include "bolt/exec/Exchange.h"
namespace bytedance::bolt::exec::test {

/// Given taskId that starts with local:// returns an instance of ExchangeSource
/// that fetches data from local OutputBufferManager.
std::unique_ptr<exec::ExchangeSource> createLocalExchangeSource(
    const std::string& taskId,
    int destination,
    std::shared_ptr<exec::ExchangeQueue> queue,
    memory::MemoryPool* pool);

/// Ensures that there are no references to ExchangeSource callbacks,
/// e.g. while waiting for timing out. Call this before end of unit
/// tests to ensure no ASAN errors at exit.
void testingShutdownLocalExchangeSource();

} // namespace bytedance::bolt::exec::test
