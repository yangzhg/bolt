#include "bolt/tool/boltfs/Executor.h"
#include "bolt/tool/boltfs/Catalog.h"

#include "bolt/exec/tests/utils/QueryAssertions.h"

#include <gtest/gtest.h>

namespace bytedance::bolt::tool::boltfs {
namespace {

TEST(ExecutorTest, SampleReturnsRows) {
  Catalog catalog;
  Executor executor;
  const auto table = catalog.resolveTable(BoltFsPath{
      "boltfs://warehouse/tpch/nation", {"warehouse", "tpch", "nation"}});

  QuerySpec query;
  query.limit = 3;
  const auto result = executor.sample(table, query);
  EXPECT_EQ(result.rowCount, 3);
  EXPECT_TRUE(result.rowType->containsChild("n_name"));
}

TEST(ExecutorTest, CatSupportsProjectionFilterAndAggregation) {
  Catalog catalog;
  Executor executor;
  const auto table = catalog.resolveTable(BoltFsPath{
      "boltfs://warehouse/tpch/orders", {"warehouse", "tpch", "orders"}});

  QuerySpec projectionQuery;
  projectionQuery.columns = {"o_orderstatus", "o_totalprice"};
  projectionQuery.filter = "o_orderstatus = 'F'";
  projectionQuery.limit = 5;
  const auto projected = executor.cat(table, projectionQuery);
  EXPECT_EQ(projected.rowCount, 5);
  EXPECT_EQ(projected.rowType->size(), 2);
  EXPECT_EQ(projected.rowType->nameOf(0), "o_orderstatus");

  QuerySpec aggregationQuery;
  aggregationQuery.filter = "o_orderstatus = 'F'";
  aggregationQuery.groupBy = {"o_orderstatus"};
  aggregationQuery.metrics = {"count(*)", "sum(o_totalprice)"};
  aggregationQuery.limit = 5;
  const auto aggregated = executor.cat(table, aggregationQuery);
  EXPECT_TRUE(aggregated.aggregated);
  EXPECT_GE(aggregated.rowCount, 1);
  EXPECT_TRUE(aggregated.rowType->containsChild("o_orderstatus"));
  EXPECT_TRUE(aggregated.rowType->containsChild("count_star"));
  EXPECT_TRUE(aggregated.rowType->containsChild("sum_o_totalprice"));
}

TEST(ExecutorTest, DemoBackendSupportsProjectionAndAggregation) {
  Catalog catalog;
  Executor executor;
  const auto table = catalog.resolveTable(BoltFsPath{
      "boltfs://warehouse/demo/error_events",
      {"warehouse", "demo", "error_events"}});

  QuerySpec projectionQuery;
  projectionQuery.columns = {"region", "error_code", "latency_ms"};
  projectionQuery.filter = "dt = '2026-03-24' AND status = 500";
  projectionQuery.limit = 3;
  const auto projected = executor.cat(table, projectionQuery);
  EXPECT_EQ(projected.rowType->size(), 3);
  EXPECT_EQ(projected.rowType->nameOf(0), "region");
  EXPECT_GE(projected.rowCount, 1);

  QuerySpec aggregationQuery;
  aggregationQuery.filter = "dt = '2026-03-24' AND status = 500";
  aggregationQuery.groupBy = {"region"};
  aggregationQuery.metrics = {"count(*)", "avg(latency_ms)"};
  aggregationQuery.limit = 5;
  const auto aggregated = executor.cat(table, aggregationQuery);
  EXPECT_TRUE(aggregated.aggregated);
  EXPECT_TRUE(aggregated.rowType->containsChild("region"));
  EXPECT_TRUE(aggregated.rowType->containsChild("count_star"));
  EXPECT_TRUE(aggregated.rowType->containsChild("avg_latency_ms"));
}

} // namespace
} // namespace bytedance::bolt::tool::boltfs
