#include "bolt/tool/boltfs/Executor.h"

#include <fmt/format.h>

#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/Connector.h"
#include "bolt/connectors/tpch/TpchConnector.h"
#include "bolt/connectors/tpch/TpchConnectorSplit.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/parse/TypeResolver.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

#include <folly/executors/InlineExecutor.h>
#include <algorithm>
#include <atomic>
#include <mutex>
#include <regex>
#include <unordered_map>
#include <variant>

namespace bytedance::bolt::tool::boltfs {
namespace {

const std::string kTpchConnectorId = connector::tpch::kBoltTpchConnectorId;
using DemoValue = std::variant<std::string, int64_t, double>;

struct DemoRow {
  std::unordered_map<std::string, DemoValue> values;
};

struct DemoMetric {
  std::string expression;
  std::string alias;
  std::string op;
  std::string column;
};

struct DemoClause {
  std::string column;
  std::string op;
  std::string literal;
  bool numeric{false};
  double numericValue{0};
};

void initializeRuntime() {
  static std::once_flag once;
  std::call_once(once, [] {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::initialize(memory::MemoryManager::Options{});
    }

    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();

    if (!connector::isConnectorRegistered(kTpchConnectorId)) {
      auto tpchConnector =
          connector::getConnectorFactory(connector::kTpchConnectorName)
              ->newConnector(
                  kTpchConnectorId,
                  std::make_shared<config::ConfigBase>(
                      std::unordered_map<std::string, std::string>{}));
      connector::registerConnector(tpchConnector);
    }
  });
}

std::vector<std::string> allColumns(const ResolvedTable& table) {
  return table.schema->names();
}

uint64_t effectiveLimit(uint64_t limit, uint64_t defaultLimit) {
  return limit == 0 ? defaultLimit : limit;
}

std::vector<std::string> metricAliases(
    const std::vector<std::string>& metrics) {
  static const std::regex kMetricPattern(
      R"(^(count\(\*\)|(sum|avg|min|max)\(([A-Za-z_][A-Za-z0-9_]*)\))$)");

  std::vector<std::string> aliased;
  aliased.reserve(metrics.size());
  std::smatch match;
  for (const auto& metric : metrics) {
    if (metric == "count(*)") {
      aliased.push_back("count(*) AS count_star");
      continue;
    }
    std::regex_match(metric, match, kMetricPattern);
    aliased.push_back(
        fmt::format("{} AS {}_{}", metric, match[2].str(), match[3].str()));
  }
  return aliased;
}

QueryResult makeResult(
    std::shared_ptr<memory::MemoryPool> outputPool,
    std::shared_ptr<core::QueryCtx> queryCtx,
    std::shared_ptr<exec::Task> task,
    std::vector<RowVectorPtr> rows,
    uint64_t requestedLimit,
    bool aggregated,
    OutputFormat format);

std::vector<DemoRow> demoRows(const std::string& tableName) {
  if (tableName == "error_events") {
    return {
        {{{"dt", "2026-03-24"},
          {"region", "us-east"},
          {"status", int64_t{500}},
          {"error_code", "MODEL_TIMEOUT"},
          {"latency_ms", int64_t{1720}},
          {"agent_id", "agent-risk"},
          {"tool_name", "sql_runner"}}},
        {{{"dt", "2026-03-24"},
          {"region", "us-east"},
          {"status", int64_t{500}},
          {"error_code", "MODEL_TIMEOUT"},
          {"latency_ms", int64_t{1490}},
          {"agent_id", "agent-risk"},
          {"tool_name", "sql_runner"}}},
        {{{"dt", "2026-03-24"},
          {"region", "eu-west"},
          {"status", int64_t{500}},
          {"error_code", "TOOL_UNAVAILABLE"},
          {"latency_ms", int64_t{2100}},
          {"agent_id", "agent-sales"},
          {"tool_name", "doc_search"}}},
        {{{"dt", "2026-03-24"},
          {"region", "eu-west"},
          {"status", int64_t{404}},
          {"error_code", "NOT_FOUND"},
          {"latency_ms", int64_t{120}},
          {"agent_id", "agent-sales"},
          {"tool_name", "doc_search"}}},
        {{{"dt", "2026-03-24"},
          {"region", "ap-sg"},
          {"status", int64_t{500}},
          {"error_code", "RATE_LIMITED"},
          {"latency_ms", int64_t{980}},
          {"agent_id", "agent-ops"},
          {"tool_name", "browser"}}},
        {{{"dt", "2026-03-23"},
          {"region", "us-east"},
          {"status", int64_t{500}},
          {"error_code", "MODEL_TIMEOUT"},
          {"latency_ms", int64_t{1880}},
          {"agent_id", "agent-risk"},
          {"tool_name", "sql_runner"}}},
        {{{"dt", "2026-03-23"},
          {"region", "eu-west"},
          {"status", int64_t{500}},
          {"error_code", "TOOL_UNAVAILABLE"},
          {"latency_ms", int64_t{2310}},
          {"agent_id", "agent-sales"},
          {"tool_name", "doc_search"}}},
        {{{"dt", "2026-03-23"},
          {"region", "ap-sg"},
          {"status", int64_t{200}},
          {"error_code", "OK"},
          {"latency_ms", int64_t{60}},
          {"agent_id", "agent-ops"},
          {"tool_name", "browser"}}}};
  }

  if (tableName == "workflow_runs") {
    return {
        {{{"dt", "2026-03-24"},
          {"team", "risk"},
          {"workflow_name", "daily_triage"},
          {"run_status", "success"},
          {"duration_ms", int64_t{4200}},
          {"saved_minutes", int64_t{45}}}},
        {{{"dt", "2026-03-24"},
          {"team", "risk"},
          {"workflow_name", "counterparty_check"},
          {"run_status", "success"},
          {"duration_ms", int64_t{6100}},
          {"saved_minutes", int64_t{70}}}},
        {{{"dt", "2026-03-24"},
          {"team", "ops"},
          {"workflow_name", "incident_summary"},
          {"run_status", "failed"},
          {"duration_ms", int64_t{8500}},
          {"saved_minutes", int64_t{0}}}},
        {{{"dt", "2026-03-23"},
          {"team", "risk"},
          {"workflow_name", "daily_triage"},
          {"run_status", "success"},
          {"duration_ms", int64_t{4000}},
          {"saved_minutes", int64_t{40}}}},
        {{{"dt", "2026-03-23"},
          {"team", "ops"},
          {"workflow_name", "incident_summary"},
          {"run_status", "success"},
          {"duration_ms", int64_t{8200}},
          {"saved_minutes", int64_t{65}}}},
        {{{"dt", "2026-03-23"},
          {"team", "sales"},
          {"workflow_name", "lead_cleanup"},
          {"run_status", "success"},
          {"duration_ms", int64_t{3000}},
          {"saved_minutes", int64_t{30}}}}};
  }

  throw std::runtime_error(fmt::format("Unknown demo table '{}'", tableName));
}

std::string demoValueToString(const DemoValue& value) {
  if (std::holds_alternative<std::string>(value)) {
    return std::get<std::string>(value);
  }
  if (std::holds_alternative<int64_t>(value)) {
    return fmt::format("{}", std::get<int64_t>(value));
  }
  return fmt::format("{}", std::get<double>(value));
}

double demoValueToDouble(const DemoValue& value) {
  if (std::holds_alternative<int64_t>(value)) {
    return static_cast<double>(std::get<int64_t>(value));
  }
  if (std::holds_alternative<double>(value)) {
    return std::get<double>(value);
  }
  return std::stod(std::get<std::string>(value));
}

int64_t demoValueToInt64(const DemoValue& value) {
  if (std::holds_alternative<int64_t>(value)) {
    return std::get<int64_t>(value);
  }
  if (std::holds_alternative<double>(value)) {
    return static_cast<int64_t>(std::get<double>(value));
  }
  return std::stoll(std::get<std::string>(value));
}

std::vector<DemoClause> parseDemoClauses(const std::string& filter) {
  if (filter.empty()) {
    return {};
  }

  static const std::regex kClausePattern(
      R"(^([A-Za-z_][A-Za-z0-9_]*)\s*(=|!=|>=|<=|>|<)\s*('[^']*'|-?[0-9]+(\.[0-9]+)?)$)");
  static const std::regex kAndPattern(
      R"(\s+AND\s+)", std::regex_constants::icase);
  std::vector<DemoClause> clauses;
  std::string remaining = filter;
  std::smatch splitMatch;
  std::smatch clauseMatch;

  while (std::regex_search(remaining, splitMatch, kAndPattern)) {
    const auto clause = splitMatch.prefix().str();
    std::regex_match(clause, clauseMatch, kClausePattern);
    auto literal = clauseMatch[3].str();
    const bool numeric = !literal.empty() && literal.front() != '\'';
    if (!numeric) {
      literal = literal.substr(1, literal.size() - 2);
    }
    clauses.push_back(
        {clauseMatch[1].str(),
         clauseMatch[2].str(),
         literal,
         numeric,
         numeric ? std::stod(literal) : 0});
    remaining = splitMatch.suffix().str();
  }

  std::regex_match(remaining, clauseMatch, kClausePattern);
  auto literal = clauseMatch[3].str();
  const bool numeric = !literal.empty() && literal.front() != '\'';
  if (!numeric) {
    literal = literal.substr(1, literal.size() - 2);
  }
  clauses.push_back(
      {clauseMatch[1].str(),
       clauseMatch[2].str(),
       literal,
       numeric,
       numeric ? std::stod(literal) : 0});
  return clauses;
}

bool matchesClause(const DemoValue& value, const DemoClause& clause) {
  if (clause.numeric) {
    const auto current = demoValueToDouble(value);
    if (clause.op == "=") {
      return current == clause.numericValue;
    }
    if (clause.op == "!=") {
      return current != clause.numericValue;
    }
    if (clause.op == ">") {
      return current > clause.numericValue;
    }
    if (clause.op == "<") {
      return current < clause.numericValue;
    }
    if (clause.op == ">=") {
      return current >= clause.numericValue;
    }
    return current <= clause.numericValue;
  }

  const auto current = demoValueToString(value);
  if (clause.op == "=") {
    return current == clause.literal;
  }
  if (clause.op == "!=") {
    return current != clause.literal;
  }
  if (clause.op == ">") {
    return current > clause.literal;
  }
  if (clause.op == "<") {
    return current < clause.literal;
  }
  if (clause.op == ">=") {
    return current >= clause.literal;
  }
  return current <= clause.literal;
}

std::vector<DemoRow> filterDemoRows(
    const std::vector<DemoRow>& rows,
    const std::string& filter) {
  const auto clauses = parseDemoClauses(filter);
  if (clauses.empty()) {
    return rows;
  }

  std::vector<DemoRow> filtered;
  for (const auto& row : rows) {
    bool matches = true;
    for (const auto& clause : clauses) {
      if (!matchesClause(row.values.at(clause.column), clause)) {
        matches = false;
        break;
      }
    }
    if (matches) {
      filtered.push_back(row);
    }
  }
  return filtered;
}

std::vector<DemoMetric> parseDemoMetrics(
    const std::vector<std::string>& metrics) {
  static const std::regex kMetricPattern(
      R"(^(count\(\*\)|(sum|avg|min|max)\(([A-Za-z_][A-Za-z0-9_]*)\))$)");
  std::vector<DemoMetric> parsed;
  std::smatch match;
  for (const auto& metric : metrics) {
    if (metric == "count(*)") {
      parsed.push_back({metric, "count_star", "count", ""});
      continue;
    }
    std::regex_match(metric, match, kMetricPattern);
    parsed.push_back(
        {metric,
         fmt::format("{}_{}", match[2].str(), match[3].str()),
         match[2].str(),
         match[3].str()});
  }
  return parsed;
}

RowVectorPtr demoRowsToVector(
    const RowTypePtr& rowType,
    const std::vector<DemoRow>& rows,
    memory::MemoryPool* pool) {
  test::VectorMaker maker(pool);
  std::vector<VectorPtr> children;
  children.reserve(rowType->size());

  for (auto i = 0; i < rowType->size(); ++i) {
    const auto& name = rowType->nameOf(i);
    const auto& type = rowType->childAt(i);
    if (type->kind() == TypeKind::VARCHAR) {
      std::vector<std::string> data;
      data.reserve(rows.size());
      for (const auto& row : rows) {
        data.push_back(demoValueToString(row.values.at(name)));
      }
      children.push_back(maker.flatVector(data, type));
    } else if (type->kind() == TypeKind::BIGINT) {
      std::vector<int64_t> data;
      data.reserve(rows.size());
      for (const auto& row : rows) {
        data.push_back(demoValueToInt64(row.values.at(name)));
      }
      children.push_back(maker.flatVector<int64_t>(data, type));
    } else if (type->kind() == TypeKind::INTEGER) {
      std::vector<int32_t> data;
      data.reserve(rows.size());
      for (const auto& row : rows) {
        data.push_back(
            static_cast<int32_t>(demoValueToInt64(row.values.at(name))));
      }
      children.push_back(maker.flatVector<int32_t>(data, type));
    } else {
      std::vector<double> data;
      data.reserve(rows.size());
      for (const auto& row : rows) {
        data.push_back(demoValueToDouble(row.values.at(name)));
      }
      children.push_back(maker.flatVector<double>(data, type));
    }
  }
  return maker.rowVector(rowType->names(), children);
}

QueryResult demoSampleResult(
    const ResolvedTable& table,
    const QuerySpec& query) {
  const auto limit = effectiveLimit(query.limit, kDefaultSampleLimit);
  static std::atomic_uint64_t poolId{200000};
  auto rows = demoRows(table.tableName);
  if (rows.size() > limit) {
    rows.resize(limit);
  }
  auto* outputPoolHolder = new std::shared_ptr<memory::MemoryPool>(
      memory::memoryManager()->addLeafPool(
          fmt::format("boltfs_demo_output_sample_{}", poolId++)));
  auto batch = demoRowsToVector(table.schema, rows, outputPoolHolder->get());
  return makeResult(
      *outputPoolHolder,
      nullptr,
      nullptr,
      {batch},
      limit,
      false,
      OutputFormat::kJson);
}

QueryResult demoCatResult(const ResolvedTable& table, const QuerySpec& query) {
  const auto limit = effectiveLimit(query.limit, kDefaultCatLimit);
  static std::atomic_uint64_t poolId{300000};
  auto rows = filterDemoRows(demoRows(table.tableName), query.filter);
  const bool aggregated = !query.metrics.empty();

  RowTypePtr rowType;
  std::vector<DemoRow> outputRows;
  if (aggregated) {
    const auto metrics = parseDemoMetrics(query.metrics);
    std::unordered_map<std::string, DemoRow> groups;
    std::unordered_map<std::string, int64_t> counts;

    for (const auto& row : rows) {
      std::string key;
      DemoRow grouped;
      for (const auto& column : query.groupBy) {
        grouped.values[column] = row.values.at(column);
        key += demoValueToString(row.values.at(column));
        key += "|";
      }
      auto& agg = groups[key];
      for (const auto& column : query.groupBy) {
        agg.values[column] = row.values.at(column);
      }
      counts[key]++;
      for (const auto& metric : metrics) {
        if (metric.op == "count") {
          agg.values[metric.alias] = counts[key];
          continue;
        }
        const auto value = demoValueToDouble(row.values.at(metric.column));
        if (!agg.values.count(metric.alias)) {
          agg.values[metric.alias] = value;
        } else if (metric.op == "sum" || metric.op == "avg") {
          agg.values[metric.alias] =
              demoValueToDouble(agg.values[metric.alias]) + value;
        } else if (metric.op == "min") {
          agg.values[metric.alias] =
              std::min(demoValueToDouble(agg.values[metric.alias]), value);
        } else if (metric.op == "max") {
          agg.values[metric.alias] =
              std::max(demoValueToDouble(agg.values[metric.alias]), value);
        }
      }
    }

    for (auto& entry : groups) {
      auto& grouped = entry.second;
      for (const auto& metric : metrics) {
        if (metric.op == "avg") {
          const auto groupKey = [&]() {
            std::string composed;
            for (const auto& column : query.groupBy) {
              composed += demoValueToString(grouped.values[column]);
              composed += "|";
            }
            return composed;
          }();
          grouped.values[metric.alias] =
              demoValueToDouble(grouped.values[metric.alias]) /
              static_cast<double>(counts[groupKey]);
        }
      }
      outputRows.push_back(grouped);
    }

    std::vector<std::string> names = query.groupBy;
    std::vector<TypePtr> types;
    for (const auto& column : query.groupBy) {
      types.push_back(table.schema->findChild(column));
    }
    for (const auto& metric : metrics) {
      names.push_back(metric.alias);
      types.push_back(
          metric.op == "count" ? TypePtr(BIGINT()) : TypePtr(DOUBLE()));
    }
    rowType = ROW(std::move(names), std::move(types));
  } else {
    auto selectedColumns =
        query.columns.empty() ? table.schema->names() : query.columns;
    for (const auto& row : rows) {
      DemoRow projected;
      for (const auto& column : selectedColumns) {
        projected.values[column] = row.values.at(column);
      }
      outputRows.push_back(std::move(projected));
    }
    std::vector<TypePtr> types;
    for (const auto& column : selectedColumns) {
      types.push_back(table.schema->findChild(column));
    }
    rowType = ROW(std::move(selectedColumns), std::move(types));
  }

  if (outputRows.size() > limit) {
    outputRows.resize(limit);
  }
  auto* outputPoolHolder = new std::shared_ptr<memory::MemoryPool>(
      memory::memoryManager()->addLeafPool(
          fmt::format("boltfs_demo_output_cat_{}", poolId++)));
  auto batch = demoRowsToVector(rowType, outputRows, outputPoolHolder->get());
  return makeResult(
      *outputPoolHolder,
      nullptr,
      nullptr,
      {batch},
      limit,
      aggregated,
      query.format);
}

QueryResult makeResult(
    std::shared_ptr<memory::MemoryPool> outputPool,
    std::shared_ptr<core::QueryCtx> queryCtx,
    std::shared_ptr<exec::Task> task,
    std::vector<RowVectorPtr> rows,
    uint64_t requestedLimit,
    bool aggregated,
    OutputFormat format) {
  QueryResult result;
  result.poolHolder = std::move(outputPool);
  result.queryCtxHolder = std::move(queryCtx);
  result.taskHolder = std::move(task);
  if (!rows.empty()) {
    result.rowType = asRowType(rows.front()->type());
  } else {
    result.rowType = ROW({}, {});
  }
  for (const auto& batch : rows) {
    result.rowCount += batch->size();
  }
  result.limit = requestedLimit;
  result.truncated = result.rowCount >= requestedLimit;
  result.aggregated = aggregated;
  result.format = format;
  result.batches = std::move(rows);
  return result;
}

exec::Split makeTpchSplit() {
  return exec::Split(
      std::make_shared<connector::tpch::TpchConnectorSplit>(kTpchConnectorId));
}

QueryResult collectResults(
    std::shared_ptr<exec::Task> task,
    std::shared_ptr<core::QueryCtx> queryCtx,
    exec::test::PlanBuilder& builder,
    uint64_t queryIndex,
    uint64_t requestedLimit,
    bool aggregated,
    OutputFormat format) {
  auto* outputPoolHolder = new std::shared_ptr<memory::MemoryPool>(
      memory::memoryManager()->addLeafPool(
          fmt::format("boltfs_output_{}", queryIndex)));
  std::vector<RowVectorPtr> copiedBatches;
  while (auto batch = task->next()) {
    auto copied = BaseVector::create(
        batch->type(), batch->size(), outputPoolHolder->get());
    copied->copy(batch.get(), 0, 0, batch->size());
    copiedBatches.push_back(std::dynamic_pointer_cast<RowVector>(copied));
  }
  if (copiedBatches.empty()) {
    copiedBatches.push_back(std::dynamic_pointer_cast<RowVector>(
        BaseVector::create(ROW({}, {}), 0, outputPoolHolder->get())));
  }

  return makeResult(
      *outputPoolHolder,
      std::move(queryCtx),
      std::move(task),
      std::move(copiedBatches),
      requestedLimit,
      aggregated,
      format);
}

QueryResult runPlan(
    exec::test::PlanBuilder& builder,
    const core::PlanNodeId& scanNodeId,
    uint64_t requestedLimit,
    bool aggregated,
    OutputFormat format) {
  static std::atomic_uint64_t queryId{0};
  const auto queryIndex = queryId++;

  auto* executor = &folly::InlineExecutor::instance();
  auto queryCtx = core::QueryCtx::create(executor);
  // Bolt task teardown still has lifecycle issues in this standalone tool path.
  // For the hackathon MVP we intentionally keep these process-scoped so the
  // CLI can return results reliably.
  auto task = exec::Task::create(
      fmt::format("boltfs_{}", queryIndex),
      builder.planFragment(),
      0,
      queryCtx,
      exec::Task::ExecutionMode::kSerial,
      exec::Consumer{});

  task->addSplit(scanNodeId, makeTpchSplit());
  task->noMoreSplits(scanNodeId);
  return collectResults(
      std::move(task),
      std::move(queryCtx),
      builder,
      queryIndex,
      requestedLimit,
      aggregated,
      format);
}

} // namespace

Executor::Executor() {
  initializeRuntime();
}

QueryResult Executor::sample(const ResolvedTable& table, const QuerySpec& query)
    const {
  if (table.backend == BackendKind::kDemo) {
    return demoSampleResult(table, query);
  }
  const auto limit = effectiveLimit(query.limit, kDefaultSampleLimit);
  core::PlanNodeId scanNodeId;
  auto builder =
      exec::test::PlanBuilder()
          .tpchTableScan(table.table, allColumns(table), 1.0, kTpchConnectorId)
          .capturePlanNodeId(scanNodeId)
          .limit(0, limit, false);

  return runPlan(builder, scanNodeId, limit, false, OutputFormat::kJson);
}

QueryResult Executor::cat(const ResolvedTable& table, const QuerySpec& query)
    const {
  if (table.backend == BackendKind::kDemo) {
    return demoCatResult(table, query);
  }
  const auto limit = effectiveLimit(query.limit, kDefaultCatLimit);
  core::PlanNodeId scanNodeId;
  auto builder = exec::test::PlanBuilder().tpchTableScan(
      table.table, allColumns(table), 1.0, kTpchConnectorId);
  builder.capturePlanNodeId(scanNodeId);

  if (!query.filter.empty()) {
    builder.filter(query.filter);
  }

  const bool aggregated = !query.metrics.empty();
  if (aggregated) {
    builder.singleAggregation(query.groupBy, metricAliases(query.metrics));
  } else if (!query.columns.empty()) {
    builder.project(query.columns);
  }

  builder.limit(0, limit, false);
  return runPlan(builder, scanNodeId, limit, aggregated, query.format);
}

} // namespace bytedance::bolt::tool::boltfs
