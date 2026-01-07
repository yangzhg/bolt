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

#include <memory>
#include <vector>

#include "BenchmarkBase.h"

#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/tpch/gen/TpchGen.h"

namespace bolt::cudf::benchmark {

template <bool useGPU>
class AggregationBenchmark : public BenchmarkBase<useGPU> {
 public:
  explicit AggregationBenchmark(
      std::shared_ptr<std::vector<bytedance::bolt::RowVectorPtr>> input)
      : BenchmarkBase<useGPU>(input) {
    bytedance::bolt::aggregate::prestosql::registerAllAggregateFunctions();
    bytedance::bolt::functions::prestosql::registerAllScalarFunctions();
    bytedance::bolt::parse::registerTypeResolver();

    auto planBuilder =
        bytedance::bolt::exec::test::PlanBuilder().values(*this->input_);
    if (this->input_->size() == 1) {
      planBuilder = planBuilder.localPartitionRoundRobinRow();
    } else {
      planBuilder = planBuilder.localPartitionRoundRobin();
    }
    planBuilder =
        planBuilder
            .project(
                {"l_returnflag",
                 "l_linestatus",
                 "l_quantity",
                 "l_extendedprice",
                 "l_extendedprice * (1.0 - l_discount) AS l_sum_disc_price",
                 "l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax) AS l_sum_charge",
                 "l_discount"})
            .partialAggregation(
                {"l_returnflag", "l_linestatus"},
                {"sum(l_quantity)",
                 "sum(l_extendedprice)",
                 "sum(l_sum_disc_price)",
                 "sum(l_sum_charge)",
                 "avg(l_quantity)",
                 "avg(l_extendedprice)",
                 "avg(l_discount)",
                 "count(0)"},
                {},
                useGPU);
    planBuilder = planBuilder.localPartition({});
    planBuilder = planBuilder.finalAggregation();
    this->plan_ = planBuilder.planNode();
  }
};

} // namespace bolt::cudf::benchmark