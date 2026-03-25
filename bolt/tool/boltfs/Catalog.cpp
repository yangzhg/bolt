#include "bolt/tool/boltfs/Catalog.h"

#include <fmt/format.h>

#include <stdexcept>

namespace bytedance::bolt::tool::boltfs {
namespace {

RowTypePtr demoSchema(std::string_view tableName) {
  if (tableName == "error_events") {
    return ROW(
        {"dt",
         "region",
         "status",
         "error_code",
         "latency_ms",
         "agent_id",
         "tool_name"},
        {VARCHAR(),
         VARCHAR(),
         INTEGER(),
         VARCHAR(),
         INTEGER(),
         VARCHAR(),
         VARCHAR()});
  }
  if (tableName == "workflow_runs") {
    return ROW(
        {"dt",
         "team",
         "workflow_name",
         "run_status",
         "duration_ms",
         "saved_minutes"},
        {VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), INTEGER(), INTEGER()});
  }
  throw std::runtime_error(fmt::format("Unknown demo table '{}'", tableName));
}

void requirePrefix(const BoltFsPath& path) {
  if (!path.segments.empty() && path.segments[0] != kWarehouse) {
    throw std::runtime_error(
        fmt::format("Unknown namespace '{}'", path.segments[0]));
  }
  if (path.segments.size() >= 2 && path.segments[1] != kTpchBackend &&
      path.segments[1] != kDemoBackend) {
    throw std::runtime_error(
        fmt::format("Unknown backend '{}'", path.segments[1]));
  }
}

} // namespace

std::vector<CatalogEntry> Catalog::list(const BoltFsPath& path) const {
  requirePrefix(path);
  if (path.segments.empty()) {
    return {{kWarehouse, "namespace", "boltfs://warehouse"}};
  }
  if (path.segments.size() == 1) {
    return {
        {kTpchBackend, "backend", "boltfs://warehouse/tpch"},
        {kDemoBackend, "backend", "boltfs://warehouse/demo"}};
  }
  if (path.segments.size() == 2) {
    if (path.segments[1] == kDemoBackend) {
      return {
          {"error_events", "table", "boltfs://warehouse/demo/error_events"},
          {"workflow_runs", "table", "boltfs://warehouse/demo/workflow_runs"}};
    }
    std::vector<CatalogEntry> entries;
    for (const auto table : tpch::tables) {
      const auto tableName = std::string{tpch::toTableName(table)};
      entries.push_back(CatalogEntry{
          tableName,
          "table",
          fmt::format("boltfs://warehouse/tpch/{}", tableName)});
    }
    return entries;
  }
  throw std::runtime_error(
      "ls supports only root, namespace, and backend paths");
}

ResolvedTable Catalog::resolveTable(const BoltFsPath& path) const {
  requirePrefix(path);
  if (path.segments.size() != 3) {
    throw std::runtime_error(
        "Expected a table path like boltfs://warehouse/tpch/orders");
  }

  const auto tableName = path.segments[2];
  if (path.segments[1] == kDemoBackend) {
    return ResolvedTable{
        fmt::format("boltfs://warehouse/demo/{}", tableName),
        BackendKind::kDemo,
        tpch::Table::TBL_NATION,
        tableName,
        demoSchema(tableName)};
  }
  const auto table = tpch::fromTableName(tableName);
  return ResolvedTable{
      fmt::format("boltfs://warehouse/tpch/{}", tableName),
      BackendKind::kTpch,
      table,
      tableName,
      tpch::getTableSchema(table, true)};
}

RowTypePtr Catalog::schema(const BoltFsPath& path) const {
  return resolveTable(path).schema;
}

} // namespace bytedance::bolt::tool::boltfs
