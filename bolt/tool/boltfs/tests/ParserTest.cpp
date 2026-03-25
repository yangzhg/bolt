#include "bolt/tool/boltfs/Parser.h"

#include <gtest/gtest.h>

namespace bytedance::bolt::tool::boltfs {
namespace {

TEST(ParserTest, ParseLsCommand) {
  const auto request = parseCommand("ls boltfs://warehouse/tpch");
  EXPECT_EQ(request.kind, CommandKind::kLs);
  ASSERT_EQ(request.path.segments.size(), 2);
  EXPECT_EQ(request.path.segments[0], "warehouse");
  EXPECT_EQ(request.path.segments[1], "tpch");
}

TEST(ParserTest, ParseSchemaCommand) {
  const auto request = parseCommand("schema boltfs://warehouse/tpch/orders");
  EXPECT_EQ(request.kind, CommandKind::kSchema);
  ASSERT_EQ(request.path.segments.size(), 3);
  EXPECT_EQ(request.path.segments[2], "orders");
}

TEST(ParserTest, ParseCdAndPwdCommands) {
  const auto cd = parseCommand("cd boltfs://warehouse/demo");
  EXPECT_EQ(cd.kind, CommandKind::kCd);
  ASSERT_EQ(cd.path.segments.size(), 2);
  EXPECT_EQ(cd.path.segments[1], "demo");

  const auto pwd = parseCommand("pwd");
  EXPECT_EQ(pwd.kind, CommandKind::kPwd);
}

TEST(ParserTest, ParseSampleLimit) {
  const auto request =
      parseCommand("sample boltfs://warehouse/tpch/orders?limit=3");
  EXPECT_EQ(request.kind, CommandKind::kSample);
  EXPECT_EQ(request.query.limit, 3);
}

TEST(ParserTest, ParseAskCommand) {
  const auto request = parseCommand(
      "ask find the top error regions yesterday and summarize the main error code");
  EXPECT_EQ(request.kind, CommandKind::kAsk);
  EXPECT_EQ(
      request.query.task,
      "find the top error regions yesterday and summarize the main error code");
}

TEST(ParserTest, ParseExplainCommand) {
  const auto request = parseCommand("explain last");
  EXPECT_EQ(request.kind, CommandKind::kExplain);
  EXPECT_EQ(request.query.task, "last");
}

TEST(ParserTest, ParseCatQuery) {
  const auto request = parseCommand(
      "cat boltfs://warehouse/tpch/orders?columns=o_orderstatus,o_totalprice&filter=o_orderstatus = 'F' AND o_totalprice > 1000&group_by=o_orderstatus&metrics=count(*),sum(o_totalprice)&format=json&limit=5");
  ASSERT_EQ(request.query.columns.size(), 2);
  EXPECT_EQ(request.query.columns[0], "o_orderstatus");
  EXPECT_EQ(request.query.groupBy[0], "o_orderstatus");
  ASSERT_EQ(request.query.metrics.size(), 2);
  EXPECT_EQ(request.query.format, OutputFormat::kJson);
  EXPECT_EQ(request.query.limit, 5);
}

TEST(ParserTest, ParseCatPipelineQuery) {
  const auto request = parseCommand(
      "cat boltfs://warehouse/demo/error_events | where \"dt = '2026-03-24' AND status = 500\" | group-by region | agg \"count(*),avg(latency_ms)\" | limit 3 | to json");
  EXPECT_EQ(request.kind, CommandKind::kCat);
  ASSERT_EQ(request.path.segments.size(), 3);
  EXPECT_EQ(request.path.segments[2], "error_events");
  EXPECT_EQ(request.query.filter, "dt = '2026-03-24' AND status = 500");
  ASSERT_EQ(request.query.groupBy.size(), 1);
  EXPECT_EQ(request.query.groupBy[0], "region");
  ASSERT_EQ(request.query.metrics.size(), 2);
  EXPECT_EQ(request.query.metrics[1], "avg(latency_ms)");
  EXPECT_EQ(request.query.limit, 3);
  EXPECT_EQ(request.query.format, OutputFormat::kJson);
}

TEST(ParserTest, ParseCatPipelineHeadAlias) {
  const auto request = parseCommand(
      "cat boltfs://warehouse/demo/error_events | where \"status = 500\" | head -n 2 | to json");
  EXPECT_EQ(request.kind, CommandKind::kCat);
  EXPECT_EQ(request.query.filter, "status = 500");
  EXPECT_EQ(request.query.limit, 2);
  EXPECT_EQ(request.query.format, OutputFormat::kJson);
}

TEST(ParserTest, ParseCatPipelineSelectQuery) {
  const auto request = parseCommand(
      "cat boltfs://warehouse/demo/error_events | select dt,region,error_code | limit 2 | to ndjson");
  EXPECT_EQ(request.kind, CommandKind::kCat);
  ASSERT_EQ(request.query.columns.size(), 3);
  EXPECT_EQ(request.query.columns[0], "dt");
  EXPECT_EQ(request.query.columns[2], "error_code");
  EXPECT_EQ(request.query.limit, 2);
  EXPECT_EQ(request.query.format, OutputFormat::kNdjson);
}

TEST(ParserTest, ParseSamplePipelineQuery) {
  const auto request = parseCommand(
      "sample boltfs://warehouse/demo/error_events | head -n 2 | to json");
  EXPECT_EQ(request.kind, CommandKind::kSample);
  EXPECT_EQ(request.query.limit, 2);
  EXPECT_EQ(request.query.format, OutputFormat::kJson);
}

TEST(ParserTest, ParseSchemaPipelineQuery) {
  const auto request =
      parseCommand("schema boltfs://warehouse/demo/error_events | to json");
  EXPECT_EQ(request.kind, CommandKind::kSchema);
  EXPECT_EQ(request.query.format, OutputFormat::kJson);
}

TEST(ParserTest, ParseQuotedUriInReplStyleInput) {
  const auto request = parseCommand(
      "cat \"boltfs://warehouse/demo/error_events?filter=dt = '2026-03-24' AND status = 500&group_by=region&metrics=count(*),avg(latency_ms)&format=json\"");
  EXPECT_EQ(request.kind, CommandKind::kCat);
  ASSERT_EQ(request.path.segments.size(), 3);
  EXPECT_EQ(request.path.segments[1], "demo");
  EXPECT_EQ(request.path.segments[2], "error_events");
  ASSERT_EQ(request.query.metrics.size(), 2);
  EXPECT_EQ(request.query.metrics[1], "avg(latency_ms)");
}

TEST(ParserTest, RejectPipelineWithoutPath) {
  EXPECT_THROW(
      parseCommand("cat | where \"status = 500\""), std::runtime_error);
}

TEST(ParserTest, RejectGroupByWithoutMetrics) {
  EXPECT_THROW(
      parseCommand("cat boltfs://warehouse/tpch/orders?group_by=o_orderstatus"),
      std::runtime_error);
}

TEST(ParserTest, RejectUnsupportedFilterGrammar) {
  EXPECT_THROW(
      parseCommand(
          "cat boltfs://warehouse/tpch/orders?filter=o_orderstatus = 'F' OR o_orderstatus = 'O'"),
      std::runtime_error);
}

} // namespace
} // namespace bytedance::bolt::tool::boltfs
