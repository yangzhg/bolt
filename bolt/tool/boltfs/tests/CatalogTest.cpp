#include "bolt/tool/boltfs/Catalog.h"

#include <gtest/gtest.h>

namespace bytedance::bolt::tool::boltfs {
namespace {

TEST(CatalogTest, ListsRootAndTpchTables) {
  Catalog catalog;
  const auto root = catalog.list(BoltFsPath{"boltfs://", {}});
  ASSERT_EQ(root.size(), 1);
  EXPECT_EQ(root[0].name, "warehouse");

  const auto warehouse =
      catalog.list(BoltFsPath{"boltfs://warehouse", {"warehouse"}});
  ASSERT_EQ(warehouse.size(), 2);
  EXPECT_EQ(warehouse[0].name, "tpch");
  EXPECT_EQ(warehouse[1].name, "demo");

  const auto tpch = catalog.list(
      BoltFsPath{"boltfs://warehouse/tpch", {"warehouse", "tpch"}});
  EXPECT_FALSE(tpch.empty());
  EXPECT_EQ(tpch[0].kind, "table");
}

TEST(CatalogTest, ResolvesDemoSchema) {
  Catalog catalog;
  const auto schema = catalog.schema(BoltFsPath{
      "boltfs://warehouse/demo/error_events",
      {"warehouse", "demo", "error_events"}});
  ASSERT_NE(schema, nullptr);
  EXPECT_TRUE(schema->containsChild("dt"));
  EXPECT_TRUE(schema->containsChild("region"));
  EXPECT_TRUE(schema->containsChild("error_code"));
}

TEST(CatalogTest, ResolvesOrdersSchema) {
  Catalog catalog;
  const auto schema = catalog.schema(BoltFsPath{
      "boltfs://warehouse/tpch/orders", {"warehouse", "tpch", "orders"}});
  ASSERT_NE(schema, nullptr);
  EXPECT_TRUE(schema->containsChild("o_orderstatus"));
  EXPECT_TRUE(schema->containsChild("o_totalprice"));
}

} // namespace
} // namespace bytedance::bolt::tool::boltfs
