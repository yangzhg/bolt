#include "bolt/tool/boltfs/Renderer.h"

#include <folly/json.h>
#include <algorithm>
#include <sstream>

namespace bytedance::bolt::tool::boltfs {
namespace {

std::string outputFormatName(OutputFormat format) {
  return format == OutputFormat::kJson ? "json" : "ndjson";
}

folly::dynamic renderGuardrailsObject(const GuardrailInfo& guardrails) {
  folly::dynamic root = folly::dynamic::object;
  root["uri"] = guardrails.uri;
  root["dataset_backend"] = guardrails.datasetBackend;
  root["execution_backend"] = guardrails.executionBackend;
  root["client_mode"] = guardrails.clientMode;
  root["output_format"] = guardrails.outputFormat;
  root["safety_reason"] = guardrails.safetyReason;
  root["filter"] = guardrails.filter;
  root["row_limit"] = guardrails.rowLimit;

  folly::dynamic columns = folly::dynamic::array;
  for (const auto& column : guardrails.columns) {
    columns.push_back(column);
  }
  root["columns"] = std::move(columns);

  folly::dynamic groupBy = folly::dynamic::array;
  for (const auto& column : guardrails.groupBy) {
    groupBy.push_back(column);
  }
  root["group_by"] = std::move(groupBy);

  folly::dynamic metrics = folly::dynamic::array;
  for (const auto& metric : guardrails.metrics) {
    metrics.push_back(metric);
  }
  root["metrics"] = std::move(metrics);
  return root;
}

folly::dynamic renderSchemaObject(const RowTypePtr& schema) {
  folly::dynamic columns = folly::dynamic::array;
  for (auto i = 0; i < schema->size(); ++i) {
    folly::dynamic column = folly::dynamic::object;
    column["name"] = schema->nameOf(i);
    column["type"] = schema->childAt(i)->toString();
    columns.push_back(std::move(column));
  }
  return columns;
}

folly::dynamic variantToDynamic(const variant& value, const TypePtr& type) {
  return folly::parseJson(value.toJson(type));
}

folly::dynamic
cellToDynamic(const VectorPtr& vector, const TypePtr& type, vector_size_t row) {
  if (vector->isNullAt(row)) {
    return nullptr;
  }

  if (type->kind() == TypeKind::VARCHAR) {
    return std::string(vector->as<SimpleVector<StringView>>()->valueAt(row));
  }
  if (type->kind() == TypeKind::BIGINT) {
    return vector->as<SimpleVector<int64_t>>()->valueAt(row);
  }
  if (type->kind() == TypeKind::INTEGER) {
    return vector->as<SimpleVector<int32_t>>()->valueAt(row);
  }
  if (type->kind() == TypeKind::DOUBLE) {
    return vector->as<SimpleVector<double>>()->valueAt(row);
  }
  if (type->isDate()) {
    return vector->toString(row);
  }
  return vector->toString(row);
}

folly::dynamic rowsToArray(const QueryResult& result) {
  folly::dynamic renderedRows = folly::dynamic::array;
  for (const auto& batch : result.batches) {
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      folly::dynamic object = folly::dynamic::object;
      for (auto i = 0; i < result.rowType->size(); ++i) {
        object[result.rowType->nameOf(i)] =
            cellToDynamic(batch->childAt(i), result.rowType->childAt(i), row);
      }
      renderedRows.push_back(std::move(object));
    }
  }
  return renderedRows;
}

std::string rowsToNdjson(const QueryResult& result) {
  std::string output;
  bool first = true;
  for (const auto& batch : result.batches) {
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      folly::dynamic object = folly::dynamic::object;
      for (auto i = 0; i < result.rowType->size(); ++i) {
        object[result.rowType->nameOf(i)] =
            cellToDynamic(batch->childAt(i), result.rowType->childAt(i), row);
      }
      if (!first) {
        output.push_back('\n');
      }
      first = false;
      output += folly::toJson(object);
    }
  }
  return output;
}

std::string dynamicToDisplayString(const folly::dynamic& value) {
  if (value.isNull()) {
    return "NULL";
  }
  if (value.isString()) {
    return value.getString();
  }
  return folly::toJson(value);
}

std::string variantToDisplayString(const variant& value, const TypePtr& type) {
  return dynamicToDisplayString(variantToDynamic(value, type));
}

std::string asciiTable(
    const std::vector<std::string>& headers,
    const std::vector<std::vector<std::string>>& rows) {
  std::vector<size_t> widths(headers.size(), 0);
  for (size_t i = 0; i < headers.size(); ++i) {
    widths[i] = headers[i].size();
  }
  for (const auto& row : rows) {
    for (size_t i = 0; i < row.size(); ++i) {
      widths[i] = std::max(widths[i], row[i].size());
    }
  }

  auto border = [&]() {
    std::string out = "+";
    for (const auto width : widths) {
      out += std::string(width + 2, '-');
      out += "+";
    }
    return out;
  };

  auto renderRow = [&](const std::vector<std::string>& row) {
    std::string out = "|";
    for (size_t i = 0; i < row.size(); ++i) {
      out += " ";
      out += row[i];
      out += std::string(widths[i] - row[i].size(), ' ');
      out += " |";
    }
    return out;
  };

  std::ostringstream out;
  out << border() << '\n';
  out << renderRow(headers) << '\n';
  out << border();
  for (const auto& row : rows) {
    out << '\n' << renderRow(row);
  }
  out << '\n' << border();
  return out.str();
}

std::string renderLsHuman(const std::vector<CatalogEntry>& entries) {
  std::vector<std::vector<std::string>> rows;
  rows.reserve(entries.size());
  for (const auto& entry : entries) {
    rows.push_back({entry.name, entry.kind, entry.uri});
  }
  return asciiTable({"name", "kind", "uri"}, rows);
}

std::string renderSchemaHuman(const RowTypePtr& schema) {
  std::vector<std::vector<std::string>> rows;
  rows.reserve(schema->size());
  for (auto i = 0; i < schema->size(); ++i) {
    rows.push_back({schema->nameOf(i), schema->childAt(i)->toString()});
  }
  return asciiTable({"column", "type"}, rows);
}

std::string renderQueryHuman(const QueryResult& result) {
  std::vector<std::string> headers;
  headers.reserve(result.rowType->size());
  for (auto i = 0; i < result.rowType->size(); ++i) {
    headers.push_back(result.rowType->nameOf(i));
  }

  std::vector<std::vector<std::string>> rows;
  for (const auto& batch : result.batches) {
    for (vector_size_t row = 0; row < batch->size(); ++row) {
      std::vector<std::string> values;
      values.reserve(result.rowType->size());
      for (auto i = 0; i < result.rowType->size(); ++i) {
        values.push_back(dynamicToDisplayString(
            cellToDynamic(batch->childAt(i), result.rowType->childAt(i), row)));
      }
      rows.push_back(std::move(values));
    }
  }
  std::ostringstream out;
  out << asciiTable(headers, rows);
  out << "\nGuardrails: backend=" << result.guardrails.datasetBackend
      << ", execution=" << result.guardrails.executionBackend
      << ", row_limit=" << result.guardrails.rowLimit
      << ", format=" << result.guardrails.outputFormat;
  if (!result.guardrails.filter.empty()) {
    out << "\nFilter: " << result.guardrails.filter;
  }
  if (!result.guardrails.metrics.empty()) {
    out << "\nMetrics: ";
    for (size_t i = 0; i < result.guardrails.metrics.size(); ++i) {
      if (i > 0) {
        out << ", ";
      }
      out << result.guardrails.metrics[i];
    }
  }
  out << "\nWhy safe: " << result.guardrails.safetyReason;
  return out.str();
}

} // namespace

std::string renderLs(
    ClientMode clientMode,
    const BoltFsPath& path,
    const std::vector<CatalogEntry>& entries) {
  if (clientMode == ClientMode::kHuman) {
    return renderLsHuman(entries);
  }
  folly::dynamic root = folly::dynamic::object;
  root["command"] = "ls";
  root["path"] = path.raw.empty() ? "boltfs://" : path.raw;
  folly::dynamic renderedEntries = folly::dynamic::array;
  for (const auto& entry : entries) {
    folly::dynamic rendered = folly::dynamic::object;
    rendered["name"] = entry.name;
    rendered["kind"] = entry.kind;
    rendered["uri"] = entry.uri;
    renderedEntries.push_back(std::move(rendered));
  }
  root["entries"] = std::move(renderedEntries);
  return folly::toJson(root);
}

std::string renderSchema(
    ClientMode clientMode,
    const std::string& uri,
    const RowTypePtr& schema) {
  if (clientMode == ClientMode::kHuman) {
    return renderSchemaHuman(schema);
  }
  folly::dynamic root = folly::dynamic::object;
  root["command"] = "schema";
  root["uri"] = uri;
  root["columns"] = renderSchemaObject(schema);
  return folly::toJson(root);
}

std::string renderSample(
    ClientMode clientMode,
    const std::string& uri,
    const QueryResult& result) {
  if (clientMode == ClientMode::kHuman) {
    return renderQueryHuman(result);
  }
  folly::dynamic root = folly::dynamic::object;
  root["command"] = "sample";
  root["uri"] = uri;
  root["limit"] = result.limit;
  root["row_count"] = result.rowCount;
  root["schema"] = renderSchemaObject(result.rowType);
  root["guardrails"] = renderGuardrailsObject(result.guardrails);
  root["rows"] = rowsToArray(result);
  return folly::toJson(root);
}

std::string renderCat(
    ClientMode clientMode,
    const std::string& uri,
    const QueryResult& result) {
  if (clientMode == ClientMode::kHuman) {
    return renderQueryHuman(result);
  }
  if (result.format == OutputFormat::kJson) {
    folly::dynamic root = folly::dynamic::object;
    root["command"] = "cat";
    root["uri"] = uri;
    root["limit"] = result.limit;
    root["row_count"] = result.rowCount;
    root["aggregated"] = result.aggregated;
    root["guardrails"] = renderGuardrailsObject(result.guardrails);
    root["rows"] = rowsToArray(result);
    return folly::toJson(root);
  }

  return rowsToNdjson(result);
}

std::string renderExplain(ClientMode clientMode, const ExplainInfo& explain) {
  if (clientMode == ClientMode::kHuman) {
    std::ostringstream out;
    out << "Command: " << explain.targetCommand
        << "\nEffective Command: " << explain.effectiveCommand;
    if (!explain.task.empty()) {
      out << "\nTask: " << explain.task;
    }
    if (!explain.goal.empty()) {
      out << "\nGoal: " << explain.goal;
    }
    if (!explain.reason.empty()) {
      out << "\nWhy: " << explain.reason;
    }
    out << "\nExecution Backend: " << explain.guardrails.executionBackend
        << "\nDataset Backend: " << explain.guardrails.datasetBackend
        << "\nOutput Format: " << explain.guardrails.outputFormat
        << "\nRow Limit: " << explain.guardrails.rowLimit;
    if (!explain.guardrails.filter.empty()) {
      out << "\nFilter: " << explain.guardrails.filter;
    }
    if (!explain.guardrails.groupBy.empty()) {
      out << "\nGroup By: ";
      for (size_t i = 0; i < explain.guardrails.groupBy.size(); ++i) {
        if (i > 0) {
          out << ", ";
        }
        out << explain.guardrails.groupBy[i];
      }
    }
    if (!explain.guardrails.metrics.empty()) {
      out << "\nMetrics: ";
      for (size_t i = 0; i < explain.guardrails.metrics.size(); ++i) {
        if (i > 0) {
          out << ", ";
        }
        out << explain.guardrails.metrics[i];
      }
    }
    out << "\nWhy safe: " << explain.guardrails.safetyReason;
    return out.str();
  }

  folly::dynamic root = folly::dynamic::object;
  root["command"] = explain.command;
  root["target_command"] = explain.targetCommand;
  root["effective_command"] = explain.effectiveCommand;
  root["task"] = explain.task;
  root["goal"] = explain.goal;
  root["reason"] = explain.reason;
  root["guardrails"] = renderGuardrailsObject(explain.guardrails);
  return folly::toJson(root);
}

} // namespace bytedance::bolt::tool::boltfs
