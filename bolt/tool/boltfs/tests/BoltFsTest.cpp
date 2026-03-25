#include "bolt/tool/boltfs/BoltFs.h"

#include <gtest/gtest.h>

namespace bytedance::bolt::tool::boltfs {
namespace {

TEST(BoltFsTest, HelpAndLsWork) {
  BoltFs boltfs{ClientMode::kAgent};
  EXPECT_NE(boltfs.execute("help").find("BoltFS"), std::string::npos);
  EXPECT_NE(
      boltfs.execute("ls boltfs://warehouse/tpch").find("\"entries\""),
      std::string::npos);
}

TEST(BoltFsTest, CdPwdAndRelativePathsWork) {
  BoltFs boltfs{ClientMode::kHuman};

  EXPECT_NE(boltfs.execute("pwd").find("boltfs://"), std::string::npos);
  EXPECT_NE(
      boltfs.execute("cd boltfs://warehouse/demo")
          .find("boltfs://warehouse/demo"),
      std::string::npos);
  EXPECT_NE(
      boltfs.execute("pwd").find("boltfs://warehouse/demo"), std::string::npos);

  const auto listing = boltfs.execute("ls");
  EXPECT_NE(listing.find("error_events"), std::string::npos);

  const auto schema = boltfs.execute("schema error_events");
  EXPECT_NE(schema.find("error_code"), std::string::npos);
}

TEST(BoltFsTest, SchemaAndSampleRenderJson) {
  BoltFs boltfs{ClientMode::kAgent};
  EXPECT_NE(
      boltfs.execute("schema boltfs://warehouse/tpch/orders")
          .find("\"columns\""),
      std::string::npos);
  const auto sample =
      boltfs.execute("sample boltfs://warehouse/tpch/orders?limit=2");
  EXPECT_NE(sample.find("\"rows\""), std::string::npos);
  EXPECT_NE(sample.find("\"guardrails\""), std::string::npos);
  EXPECT_NE(sample.find("\"row_limit\""), std::string::npos);
}

TEST(BoltFsTest, CatSupportsNdjsonAndJson) {
  BoltFs boltfs{ClientMode::kAgent};
  const auto ndjson = boltfs.execute(
      "cat boltfs://warehouse/tpch/orders?filter=o_orderstatus = 'F'&group_by=o_orderstatus&metrics=count(*),sum(o_totalprice)");
  EXPECT_NE(ndjson.find("\"o_orderstatus\""), std::string::npos);
  EXPECT_NE(ndjson.find("\"count_star\""), std::string::npos);
  EXPECT_NE(ndjson.find("\"sum_o_totalprice\""), std::string::npos);

  const auto json = boltfs.execute(
      "cat boltfs://warehouse/tpch/orders?columns=o_orderstatus,o_totalprice&limit=2&format=json");
  EXPECT_NE(json.find("\"rows\""), std::string::npos);

  const auto pipeline = boltfs.execute(
      "cat boltfs://warehouse/demo/error_events | where \"dt = '2026-03-24' AND status = 500\" | group-by region | agg \"count(*),avg(latency_ms)\" | limit 3 | to json");
  EXPECT_NE(pipeline.find("\"rows\""), std::string::npos);
  EXPECT_NE(pipeline.find("\"group_by\":[\"region\"]"), std::string::npos);
  EXPECT_NE(pipeline.find("\"count_star\""), std::string::npos);
}

TEST(BoltFsTest, HumanModeRendersAsciiTables) {
  BoltFs boltfs{ClientMode::kHuman};

  const auto listing = boltfs.execute("ls boltfs://warehouse/tpch");
  EXPECT_NE(listing.find("+"), std::string::npos);
  EXPECT_NE(listing.find("name"), std::string::npos);
  EXPECT_NE(listing.find("orders"), std::string::npos);

  const auto sample =
      boltfs.execute("sample boltfs://warehouse/tpch/orders?limit=2");
  EXPECT_NE(sample.find("o_orderkey"), std::string::npos);
  EXPECT_NE(sample.find("o_orderstatus"), std::string::npos);
  EXPECT_NE(sample.find("|"), std::string::npos);
}

TEST(BoltFsTest, SampleAndSchemaPipelinesWork) {
  BoltFs boltfs{ClientMode::kAgent};

  const auto sample = boltfs.execute(
      "sample boltfs://warehouse/demo/error_events | head -n 2 | to json");
  EXPECT_NE(sample.find("\"rows\""), std::string::npos);
  EXPECT_NE(sample.find("\"row_limit\":2"), std::string::npos);

  const auto schema =
      boltfs.execute("schema boltfs://warehouse/demo/error_events | to json");
  EXPECT_NE(schema.find("\"columns\""), std::string::npos);
}

TEST(BoltFsTest, DemoBackendAndAskFlowWork) {
  BoltFs boltfs{ClientMode::kAgent};

  const auto demoList = boltfs.execute("ls boltfs://warehouse/demo");
  EXPECT_NE(demoList.find("error_events"), std::string::npos);

  const auto demoCat = boltfs.execute(
      "cat boltfs://warehouse/demo/error_events?filter=dt = '2026-03-24' AND status = 500&group_by=region&metrics=count(*),avg(latency_ms)&format=json");
  EXPECT_NE(demoCat.find("\"count_star\""), std::string::npos);
  EXPECT_NE(demoCat.find("\"avg_latency_ms\""), std::string::npos);

  const auto ask = boltfs.execute(
      "ask find the top error regions yesterday and summarize the main error code");
  EXPECT_NE(ask.find("\"task\""), std::string::npos);
  EXPECT_NE(
      ask.find("\"boltfs://warehouse/demo/error_events\""), std::string::npos);
  EXPECT_NE(ask.find("\"count_star\""), std::string::npos);
  EXPECT_NE(ask.find("\"guardrails\""), std::string::npos);
}

TEST(BoltFsTest, ExplainSupportsDirectAndLast) {
  BoltFs boltfs{ClientMode::kAgent};

  const auto direct = boltfs.execute(
      "explain cat boltfs://warehouse/demo/error_events?filter=dt = '2026-03-24' AND status = 500&group_by=region&metrics=count(*),avg(latency_ms)&format=json");
  EXPECT_NE(direct.find("\"command\":\"explain\""), std::string::npos);
  EXPECT_NE(direct.find("\"execution_backend\":\"demo\""), std::string::npos);
  EXPECT_NE(direct.find("\"group_by\""), std::string::npos);

  (void)boltfs.execute(
      "ask find the top error regions yesterday and summarize the main error code");
  const auto last = boltfs.execute("explain last");
  EXPECT_NE(last.find("\"effective_command\""), std::string::npos);
  EXPECT_NE(last.find("\"goal\""), std::string::npos);
  EXPECT_NE(last.find("use the demo error_events table"), std::string::npos);
  EXPECT_NE(last.find("\"group_by\":[\"region\"]"), std::string::npos);
}

} // namespace
} // namespace bytedance::bolt::tool::boltfs
