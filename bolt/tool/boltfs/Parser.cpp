#include "bolt/tool/boltfs/Parser.h"

#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <cstring>
#include <regex>
#include <stdexcept>

namespace bytedance::bolt::tool::boltfs {
namespace {

std::string trim(std::string_view text) {
  size_t begin = 0;
  while (begin < text.size() &&
         std::isspace(static_cast<unsigned char>(text[begin]))) {
    ++begin;
  }
  size_t end = text.size();
  while (end > begin &&
         std::isspace(static_cast<unsigned char>(text[end - 1]))) {
    --end;
  }
  return std::string{text.substr(begin, end - begin)};
}

std::string unwrapOuterQuotes(std::string_view text) {
  const auto value = trim(text);
  if (value.size() >= 2 &&
      ((value.front() == '"' && value.back() == '"') ||
       (value.front() == '\'' && value.back() == '\''))) {
    return value.substr(1, value.size() - 2);
  }
  return value;
}

std::vector<std::string> splitCommaSeparated(std::string_view text) {
  std::vector<std::string> out;
  std::string current;
  int parenDepth = 0;
  bool inSingleQuotes = false;
  for (char c : text) {
    if (c == '\'') {
      inSingleQuotes = !inSingleQuotes;
    } else if (!inSingleQuotes && c == '(') {
      ++parenDepth;
    } else if (!inSingleQuotes && c == ')') {
      --parenDepth;
    }
    if (c == ',' && !inSingleQuotes && parenDepth == 0) {
      auto token = trim(current);
      if (!token.empty()) {
        out.push_back(token);
      }
      current.clear();
      continue;
    }
    current.push_back(c);
  }
  auto token = trim(current);
  if (!token.empty()) {
    out.push_back(token);
  }
  return out;
}

std::string decodeComponent(std::string_view text) {
  std::string out;
  out.reserve(text.size());
  for (size_t i = 0; i < text.size(); ++i) {
    const char c = text[i];
    if (c == '+') {
      out.push_back(' ');
      continue;
    }
    if (c == '%' && i + 2 < text.size()) {
      const auto hex = std::string{text.substr(i + 1, 2)};
      out.push_back(static_cast<char>(std::stoi(hex, nullptr, 16)));
      i += 2;
      continue;
    }
    out.push_back(c);
  }
  return out;
}

std::vector<std::string> splitPipeline(std::string_view text) {
  std::vector<std::string> stages;
  std::string current;
  bool inSingleQuotes = false;
  bool inDoubleQuotes = false;
  int parenDepth = 0;

  for (char c : text) {
    if (c == '\'' && !inDoubleQuotes) {
      inSingleQuotes = !inSingleQuotes;
    } else if (c == '"' && !inSingleQuotes) {
      inDoubleQuotes = !inDoubleQuotes;
    } else if (!inSingleQuotes && !inDoubleQuotes && c == '(') {
      ++parenDepth;
    } else if (!inSingleQuotes && !inDoubleQuotes && c == ')') {
      --parenDepth;
    }

    if (c == '|' && !inSingleQuotes && !inDoubleQuotes && parenDepth == 0) {
      auto stage = trim(current);
      if (!stage.empty()) {
        stages.push_back(stage);
      }
      current.clear();
      continue;
    }
    current.push_back(c);
  }

  auto tail = trim(current);
  if (!tail.empty()) {
    stages.push_back(tail);
  }
  return stages;
}

BoltFsPath parsePath(std::string_view uriText) {
  const auto uri = trim(uriText);
  if (uri.rfind(kScheme, 0) != 0) {
    throw std::runtime_error(
        fmt::format("URI must start with '{}': {}", kScheme, uri));
  }

  const auto queryPos = uri.find('?');
  const auto pathText = uri.substr(
      std::strlen(kScheme),
      queryPos == std::string::npos ? std::string::npos
                                    : queryPos - std::strlen(kScheme));

  BoltFsPath path;
  path.raw = uri;
  std::string current;
  for (char c : pathText) {
    if (c == '/') {
      if (!current.empty()) {
        path.segments.push_back(current);
        current.clear();
      }
      continue;
    }
    current.push_back(c);
  }
  if (!current.empty()) {
    path.segments.push_back(current);
  }
  return path;
}

BoltFsPath parseRelativePath(std::string_view pathText) {
  const auto raw = trim(pathText);
  BoltFsPath path;
  path.raw = raw;
  std::string current;
  for (char c : raw) {
    if (c == '/') {
      if (!current.empty()) {
        path.segments.push_back(current);
        current.clear();
      }
      continue;
    }
    current.push_back(c);
  }
  if (!current.empty()) {
    path.segments.push_back(current);
  }
  return path;
}

void validateColumnName(std::string_view text, std::string_view what) {
  static const std::regex kNamePattern("^[A-Za-z_][A-Za-z0-9_]*$");
  if (!std::regex_match(text.begin(), text.end(), kNamePattern)) {
    throw std::runtime_error(fmt::format("Unsupported {} '{}'", what, text));
  }
}

void validateMetric(std::string_view metric) {
  static const std::regex kMetricPattern(
      R"(^(count\(\*\)|(sum|avg|min|max)\([A-Za-z_][A-Za-z0-9_]*\))$)");
  if (!std::regex_match(metric.begin(), metric.end(), kMetricPattern)) {
    throw std::runtime_error(fmt::format(
        "Unsupported metric '{}'. Use count(*), sum(col), avg(col), min(col), or max(col).",
        metric));
  }
}

void validateFilter(std::string_view filter) {
  static const std::regex kClausePattern(
      R"(^[A-Za-z_][A-Za-z0-9_]*\s*(=|!=|>=|<=|>|<)\s*('[^']*'|-?[0-9]+(\.[0-9]+)?|DATE\s*'[^']*')$)");
  static const std::regex kAndPattern(
      R"(\s+AND\s+)", std::regex_constants::icase);
  std::string remaining{filter};
  std::smatch match;
  while (std::regex_search(remaining, match, kAndPattern)) {
    const auto clause = trim(match.prefix().str());
    if (clause.empty()) {
      throw std::runtime_error("Empty filter clause is not allowed");
    }
    if (!std::regex_match(clause.begin(), clause.end(), kClausePattern)) {
      throw std::runtime_error(fmt::format(
          "Unsupported filter '{}'. BoltFS MVP supports only 'col op literal' clauses joined by AND.",
          clause));
    }
    remaining = match.suffix().str();
  }
  const auto clause = trim(remaining);
  if (clause.empty()) {
    throw std::runtime_error("Empty filter clause is not allowed");
  }
  if (!std::regex_match(clause.begin(), clause.end(), kClausePattern)) {
    throw std::runtime_error(fmt::format(
        "Unsupported filter '{}'. BoltFS MVP supports only 'col op literal' clauses joined by AND.",
        clause));
  }
}

QuerySpec parseQuery(std::string_view uriText) {
  QuerySpec query;
  const auto uri = trim(uriText);
  const auto queryPos = uri.find('?');
  if (queryPos == std::string::npos) {
    return query;
  }

  std::string_view params = uri;
  params.remove_prefix(queryPos + 1);
  size_t begin = 0;
  while (begin <= params.size()) {
    const auto end = params.find('&', begin);
    const auto pair = params.substr(
        begin, end == std::string::npos ? std::string::npos : end - begin);
    if (!pair.empty()) {
      const auto eq = pair.find('=');
      const auto key = trim(pair.substr(0, eq));
      const auto value = decodeComponent(
          eq == std::string::npos ? std::string_view{} : pair.substr(eq + 1));
      if (key == "columns") {
        query.columns = splitCommaSeparated(value);
        for (const auto& column : query.columns) {
          validateColumnName(column, "column");
        }
      } else if (key == "filter") {
        query.filter = trim(value);
        if (!query.filter.empty()) {
          validateFilter(query.filter);
        }
      } else if (key == "group_by") {
        query.groupBy = splitCommaSeparated(value);
        for (const auto& column : query.groupBy) {
          validateColumnName(column, "group_by column");
        }
      } else if (key == "metrics") {
        query.metrics = splitCommaSeparated(value);
        for (const auto& metric : query.metrics) {
          validateMetric(metric);
        }
      } else if (key == "limit") {
        query.limit = std::stoull(trim(value));
      } else if (key == "format") {
        if (value == "json") {
          query.format = OutputFormat::kJson;
        } else if (value == "ndjson") {
          query.format = OutputFormat::kNdjson;
        } else {
          throw std::runtime_error(
              fmt::format("Unsupported format '{}'", value));
        }
      } else if (!key.empty()) {
        throw std::runtime_error(
            fmt::format("Unsupported query parameter '{}'", key));
      }
    }
    if (end == std::string::npos) {
      break;
    }
    begin = end + 1;
  }

  if (!query.groupBy.empty() && query.metrics.empty()) {
    throw std::runtime_error("group_by requires metrics");
  }
  if (query.limit > kMaxResultRows) {
    throw std::runtime_error(fmt::format(
        "limit {} exceeds max supported row count {}",
        query.limit,
        kMaxResultRows));
  }
  return query;
}

QuerySpec parsePipelineQuery(std::string_view text) {
  QuerySpec query;
  const auto stages = splitPipeline(text);
  for (size_t i = 1; i < stages.size(); ++i) {
    const auto& stage = stages[i];
    const auto firstSpace = stage.find(' ');
    const auto op = firstSpace == std::string::npos
        ? stage
        : trim(stage.substr(0, firstSpace));
    const auto arg = firstSpace == std::string::npos
        ? std::string{}
        : unwrapOuterQuotes(trim(stage.substr(firstSpace + 1)));

    if (op == "where") {
      query.filter = arg;
      if (!query.filter.empty()) {
        validateFilter(query.filter);
      }
      continue;
    }
    if (op == "select") {
      query.columns = splitCommaSeparated(arg);
      for (const auto& column : query.columns) {
        validateColumnName(column, "column");
      }
      continue;
    }
    if (op == "group-by") {
      query.groupBy = splitCommaSeparated(arg);
      for (const auto& column : query.groupBy) {
        validateColumnName(column, "group_by column");
      }
      continue;
    }
    if (op == "agg") {
      query.metrics = splitCommaSeparated(arg);
      for (const auto& metric : query.metrics) {
        validateMetric(metric);
      }
      continue;
    }
    if (op == "limit") {
      query.limit = std::stoull(trim(arg));
      continue;
    }
    if (op == "head") {
      static const std::regex kHeadPattern(R"(^-n\s+([0-9]+)$)");
      std::smatch match;
      if (!std::regex_match(arg, match, kHeadPattern)) {
        throw std::runtime_error("head only supports '-n <rows>'");
      }
      query.limit = std::stoull(match[1].str());
      continue;
    }
    if (op == "to") {
      if (arg == "json") {
        query.format = OutputFormat::kJson;
      } else if (arg == "ndjson") {
        query.format = OutputFormat::kNdjson;
      } else {
        throw std::runtime_error(fmt::format("Unsupported format '{}'", arg));
      }
      continue;
    }
    throw std::runtime_error(
        fmt::format("Unsupported pipeline stage '{}'", op));
  }

  if (!query.groupBy.empty() && query.metrics.empty()) {
    throw std::runtime_error("group_by requires metrics");
  }
  if (query.limit > kMaxResultRows) {
    throw std::runtime_error(fmt::format(
        "limit {} exceeds max supported row count {}",
        query.limit,
        kMaxResultRows));
  }
  return query;
}

CommandKind parseCommandWord(std::string_view word) {
  if (word == "help") {
    return CommandKind::kHelp;
  }
  if (word == "ask") {
    return CommandKind::kAsk;
  }
  if (word == "explain") {
    return CommandKind::kExplain;
  }
  if (word == "ls") {
    return CommandKind::kLs;
  }
  if (word == "cd") {
    return CommandKind::kCd;
  }
  if (word == "pwd") {
    return CommandKind::kPwd;
  }
  if (word == "schema") {
    return CommandKind::kSchema;
  }
  if (word == "sample") {
    return CommandKind::kSample;
  }
  if (word == "cat") {
    return CommandKind::kCat;
  }
  if (word == "exit" || word == "quit") {
    return CommandKind::kExit;
  }
  throw std::runtime_error(fmt::format("Unknown command '{}'", word));
}

void requireTablePath(const CommandRequest& request) {
  if (request.path.raw.rfind(kScheme, 0) == 0 &&
      request.path.segments.size() < 3) {
    throw std::runtime_error(
        "This command requires a table URI like boltfs://warehouse/tpch/orders");
  }
  if (request.path.raw.rfind(kScheme, 0) != 0 &&
      request.path.segments.empty()) {
    throw std::runtime_error("Missing BoltFS URI");
  }
}

} // namespace

CommandRequest parseCommand(std::string_view text) {
  const auto input = trim(text);
  if (input.empty()) {
    return CommandRequest{CommandKind::kHelp, BoltFsPath{}, QuerySpec{}, input};
  }

  const auto firstSpace = input.find(' ');
  const auto commandText = input.substr(
      0, firstSpace == std::string::npos ? input.size() : firstSpace);
  const auto rest = firstSpace == std::string::npos
      ? std::string{}
      : trim(input.substr(firstSpace + 1));

  CommandRequest request{
      parseCommandWord(commandText), BoltFsPath{}, QuerySpec{}, input};

  switch (request.kind) {
    case CommandKind::kHelp:
    case CommandKind::kExit:
    case CommandKind::kPwd:
      return request;
    case CommandKind::kAsk:
    case CommandKind::kExplain:
      if (rest.empty()) {
        throw std::runtime_error(
            request.kind == CommandKind::kAsk ? "Missing task text for ask"
                                              : "Missing target for explain");
      }
      request.query.task = unwrapOuterQuotes(rest);
      return request;
    case CommandKind::kLs:
    case CommandKind::kCd:
      if (rest.empty()) {
        request.path = BoltFsPath{"", {}};
      } else {
        const auto target = unwrapOuterQuotes(rest);
        if (target.rfind(kScheme, 0) == 0) {
          request.path = parsePath(target);
          request.query = parseQuery(target);
        } else {
          request.path = parseRelativePath(target);
        }
      }
      return request;
    case CommandKind::kSchema:
    case CommandKind::kSample:
    case CommandKind::kCat:
      if (rest.empty()) {
        throw std::runtime_error("Missing BoltFS URI");
      }
      if (rest.find('|') != std::string::npos) {
        const auto firstPipe = rest.find('|');
        if (trim(rest.substr(0, firstPipe)).empty()) {
          throw std::runtime_error("Missing BoltFS URI");
        }
        const auto stages = splitPipeline(rest);
        if (stages.empty()) {
          throw std::runtime_error("Missing BoltFS URI");
        }
        const auto target = unwrapOuterQuotes(stages.front());
        request.path = target.rfind(kScheme, 0) == 0
            ? parsePath(target)
            : parseRelativePath(target);
        request.query = parsePipelineQuery(rest);
      } else {
        const auto target = unwrapOuterQuotes(rest);
        request.path = target.rfind(kScheme, 0) == 0
            ? parsePath(target)
            : parseRelativePath(target);
        if (target.rfind(kScheme, 0) == 0) {
          request.query = parseQuery(target);
        }
      }
      requireTablePath(request);
      return request;
  }
  throw std::runtime_error("Unsupported command");
}

std::string helpText() {
  return R"BOLTFS_HELP(BoltFS: filesystem-style data access powered by Bolt

Commands:
  ask find the top error regions yesterday
  cd boltfs://warehouse/demo
  pwd
  explain last
  explain "cat boltfs://warehouse/demo/error_events?filter=dt = '2026-03-24' AND status = 500&group_by=region&metrics=count(*),avg(latency_ms)&format=json"
  cat boltfs://warehouse/demo/error_events | where "dt = '2026-03-24' AND status = 500" | group-by region | agg "count(*),avg(latency_ms)" | limit 3 | to json
  ls boltfs://
  ls boltfs://warehouse
  ls boltfs://warehouse/tpch
  ls boltfs://warehouse/demo
  schema boltfs://warehouse/tpch/orders
  schema boltfs://warehouse/demo/error_events
  sample boltfs://warehouse/tpch/orders?limit=3
  sample boltfs://warehouse/demo/error_events?limit=3
  cat "boltfs://warehouse/tpch/orders?columns=o_orderstatus,o_totalprice&filter=o_orderstatus = 'F'&limit=5"
  cat "boltfs://warehouse/demo/error_events?filter=dt = '2026-03-24' AND status = 500&group_by=region&metrics=count(*),avg(latency_ms)&format=json"
  cat "boltfs://warehouse/tpch/orders?filter=o_orderstatus = 'F'&group_by=o_orderstatus&metrics=count(*),sum(o_totalprice)&format=ndjson"
  exit

MVP constraints:
  filter: only 'column op literal' clauses joined by AND
  metrics: count(*), sum(col), avg(col), min(col), max(col)
  pipeline stages: where, select, group-by, agg, limit, to
  max rows per request: 200
)BOLTFS_HELP";
}

} // namespace bytedance::bolt::tool::boltfs
