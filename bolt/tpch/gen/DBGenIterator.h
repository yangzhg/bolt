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

#include <memory>
#include <mutex>

#include <bolt/tpch/gen/dbgen/include/dbgen/dss.h>
#include <bolt/tpch/gen/dbgen/include/dbgen/dsstypes.h>
namespace bytedance::bolt::tpch {

/// This class exposes a thread-safe and reproducible iterator over TPC-H
/// synthetically generated data, backed by DBGEN.
class DBGenIterator {
 public:
  explicit DBGenIterator(double scaleFactor);

  // Before generating records using the gen*() functions below, call the
  // appropriate init*() function to correctly initialize the seed given the
  // offset to be generated.
  void initNation(size_t offset);
  void initRegion(size_t offset);
  void initOrder(size_t offset);
  void initSupplier(size_t offset);
  void initPart(size_t offset);
  void initCustomer(size_t offset);

  // Generate different types of records.
  void genNation(size_t index, code_t& code);
  void genRegion(size_t index, code_t& code);
  void genOrder(size_t index, order_t& order);
  void genSupplier(size_t index, supplier_t& supplier);
  void genPart(size_t index, part_t& part);
  void genCustomer(size_t index, customer_t& customer);

  DBGenContext dbgenCtx_;
};

} // namespace bytedance::bolt::tpch
