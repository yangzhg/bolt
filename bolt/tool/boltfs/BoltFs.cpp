#include "bolt/tool/boltfs/BoltFs.h"

#include "bolt/tool/boltfs/Parser.h"
#include "bolt/tool/boltfs/Renderer.h"

#include <folly/json.h>

#include <algorithm>
#include <cctype>
#include <set>
#include <stdexcept>

namespace bytedance::bolt::tool::boltfs {
namespace {

struct AskPlan {
  std::string goal;
  std::string reason;
  std::string command;
};

std::string clientModeName(ClientMode mode) {
  return mode == ClientMode::kHuman ? "human" : "agent";
}

std::string backendName(BackendKind backend) {
  return backend == BackendKind::kDemo ? kDemoBackend : kTpchBackend;
}

std::string outputFormatName(OutputFormat format) {
  return format == OutputFormat::kJson ? "json" : "ndjson";
}

std::string toLower(std::string_view text) {
  std::string lowered{text};
  std::transform(
      lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char c) {
        return std::tolower(c);
      });
  return lowered;
}

AskPlan planTask(std::string_view task) {
  const auto normalized = toLower(task);
  if (normalized.find("error") != std::string::npos &&
      normalized.find("region") != std::string::npos) {
    return {
        "find the regions with the highest operational pain",
        "use the demo error_events table and aggregate yesterday's 500s by region",
        "cat boltfs://warehouse/demo/error_events?filter=dt = '2026-03-24' AND status = 500&group_by=region&metrics=count(*),avg(latency_ms)&format=json"};
  }

  if (normalized.find("workflow") != std::string::npos ||
      normalized.find("productivity") != std::string::npos ||
      normalized.find("saved") != std::string::npos) {
    return {
        "find the workflows creating the most productivity lift",
        "aggregate saved time from workflow_runs for the latest day",
        "cat boltfs://warehouse/demo/workflow_runs?filter=dt = '2026-03-24' AND run_status = 'success'&group_by=workflow_name&metrics=count(*),sum(saved_minutes)&format=json"};
  }

  return {
      "start with a safe demo exploration",
      "fallback to a bounded sample because the task did not match a supported template",
      "sample boltfs://warehouse/demo/error_events?limit=3"};
}

folly::dynamic parseResultPayload(const std::string& output) {
  if (!output.empty() && output.front() == '{') {
    return folly::parseJson(output);
  }
  return output;
}

std::string renderAskAgent(
    const std::string& task,
    const AskPlan& plan,
    const std::string& result) {
  folly::dynamic root = folly::dynamic::object;
  root["command"] = "ask";
  root["task"] = task;
  root["goal"] = plan.goal;
  root["reason"] = plan.reason;
  root["boltfs_command"] = plan.command;
  root["result"] = parseResultPayload(result);
  return folly::toJson(root);
}

std::string renderAskHuman(
    const std::string& task,
    const AskPlan& plan,
    const std::string& result) {
  return "Task: " + task + "\nGoal: " + plan.goal + "\nWhy: " + plan.reason +
      "\nBoltFS Command: " + plan.command + "\n\n" + result;
}

GuardrailInfo makeGuardrails(
    ClientMode clientMode,
    const ResolvedTable& table,
    const QuerySpec& query,
    std::string_view safetyReason) {
  const bool isSample = query.metrics.empty() && query.columns.empty() &&
      query.groupBy.empty() && query.filter.empty();
  const auto defaultLimit = isSample ? kDefaultSampleLimit : kDefaultCatLimit;
  GuardrailInfo info;
  info.uri = table.uri;
  info.datasetBackend = backendName(table.backend);
  info.executionBackend = table.backend == BackendKind::kDemo ? "demo" : "bolt";
  info.clientMode = clientModeName(clientMode);
  info.outputFormat = outputFormatName(query.format);
  info.safetyReason = std::string(safetyReason);
  info.filter = query.filter;
  info.columns = query.columns;
  info.groupBy = query.groupBy;
  info.metrics = query.metrics;
  info.rowLimit = query.limit == 0 ? defaultLimit : query.limit;
  return info;
}

ExplainInfo explainForResolvedCommand(
    ClientMode clientMode,
    const CommandRequest& request,
    const ResolvedTable& table,
    std::string_view reason,
    std::string_view goal = {},
    std::string_view task = {},
    std::string_view effectiveCommand = {}) {
  ExplainInfo explain;
  explain.command = "explain";
  explain.targetCommand = request.originalText;
  explain.effectiveCommand = effectiveCommand.empty()
      ? request.originalText
      : std::string(effectiveCommand);
  explain.task = std::string(task);
  explain.goal = std::string(goal);
  explain.reason = std::string(reason);
  explain.guardrails = makeGuardrails(clientMode, table, request.query, reason);
  return explain;
}

} // namespace

BoltFs::BoltFs(ClientMode clientMode)
    : clientMode_(clientMode), cwd_(BoltFsPath{"boltfs://", {}}) {}

std::string BoltFs::execute(std::string_view commandLine) const {
  return executeRequest(parseCommand(commandLine));
}

std::vector<std::string> BoltFs::completeCommand(
    std::string_view prefix) const {
  static const std::vector<std::string> kCommands = {
      "ask", "cat", "explain", "exit", "help", "ls", "sample", "schema"};
  std::vector<std::string> matches;
  for (const auto& command : kCommands) {
    if (command.rfind(std::string(prefix), 0) == 0) {
      matches.push_back(command);
    }
  }
  return matches;
}

std::vector<std::string> BoltFs::completePath(std::string_view prefix) const {
  const bool quoted = !prefix.empty() && prefix.front() == '"';
  const auto rawPrefix =
      quoted ? std::string(prefix.substr(1)) : std::string(prefix);

  std::set<std::string> candidates = {
      "boltfs://",
      "boltfs://warehouse",
      "boltfs://warehouse/tpch",
      "boltfs://warehouse/demo"};
  for (const auto& entry :
       catalog_.list(parseCommand("ls boltfs://warehouse/tpch").path)) {
    candidates.insert(entry.uri);
  }
  for (const auto& entry :
       catalog_.list(parseCommand("ls boltfs://warehouse/demo").path)) {
    candidates.insert(entry.uri);
  }

  std::vector<std::string> matches;
  for (const auto& candidate : candidates) {
    if (candidate.rfind(rawPrefix, 0) == 0) {
      matches.push_back(quoted ? "\"" + candidate : candidate);
    }
  }
  return matches;
}

BoltFsPath BoltFs::resolvePath(const BoltFsPath& path) const {
  if (path.raw.empty()) {
    return cwd_;
  }
  if (path.raw.rfind(kScheme, 0) == 0) {
    return path;
  }

  BoltFsPath resolved;
  resolved.segments = cwd_.segments;
  resolved.segments.insert(
      resolved.segments.end(), path.segments.begin(), path.segments.end());
  resolved.raw = kScheme;
  for (size_t i = 0; i < resolved.segments.size(); ++i) {
    if (i > 0 || resolved.raw != kScheme) {
      resolved.raw.push_back('/');
    }
    resolved.raw += resolved.segments[i];
  }
  if (resolved.raw == kScheme) {
    resolved.raw = "boltfs://";
  }
  return resolved;
}

std::string BoltFs::executeRequest(const CommandRequest& request) const {
  switch (request.kind) {
    case CommandKind::kHelp:
      return helpText();
    case CommandKind::kExit:
      return "exit";
    case CommandKind::kPwd:
      return cwd_.raw.empty() ? "boltfs://" : cwd_.raw;
    case CommandKind::kCd: {
      const auto target = resolvePath(request.path);
      cwd_ = target.raw.empty() ? BoltFsPath{"boltfs://", {}} : target;
      return cwd_.raw.empty() ? "boltfs://" : cwd_.raw;
    }
    case CommandKind::kLs: {
      const auto path = resolvePath(request.path);
      ExplainInfo explain;
      explain.command = "explain";
      explain.targetCommand = request.originalText;
      explain.effectiveCommand = request.originalText;
      lastExplain_ = explain;
      return renderLs(clientMode_, path, catalog_.list(path));
    }
    case CommandKind::kSchema: {
      const auto path = resolvePath(request.path);
      const auto table = catalog_.resolveTable(path);
      lastExplain_ = explainForResolvedCommand(
          clientMode_,
          request,
          table,
          "schema inspection keeps agent queries typed and bounded before execution");
      if (request.query.format == OutputFormat::kJson &&
          clientMode_ == ClientMode::kHuman) {
        return renderSchema(ClientMode::kAgent, table.uri, table.schema);
      }
      return renderSchema(clientMode_, table.uri, table.schema);
    }
    case CommandKind::kSample: {
      const auto path = resolvePath(request.path);
      const auto table = catalog_.resolveTable(path);
      auto result = executor_.sample(table, request.query);
      result.guardrails = makeGuardrails(
          clientMode_,
          table,
          request.query,
          "bounded sampling limits scan scope before wider reads");
      result.guardrails.outputFormat = outputFormatName(result.format);
      lastExplain_ = explainForResolvedCommand(
          clientMode_, request, table, result.guardrails.safetyReason);
      if (request.query.format == OutputFormat::kJson &&
          clientMode_ == ClientMode::kHuman) {
        return renderSample(ClientMode::kAgent, table.uri, result);
      }
      return renderSample(clientMode_, table.uri, result);
    }
    case CommandKind::kCat: {
      const auto path = resolvePath(request.path);
      const auto table = catalog_.resolveTable(path);
      auto result = executor_.cat(table, request.query);
      result.guardrails = makeGuardrails(
          clientMode_,
          table,
          request.query,
          "constrained filters, explicit metrics, and row limits keep access predictable");
      result.guardrails.outputFormat = outputFormatName(result.format);
      lastExplain_ = explainForResolvedCommand(
          clientMode_, request, table, result.guardrails.safetyReason);
      return renderCat(clientMode_, table.uri, result);
    }
    case CommandKind::kAsk: {
      const auto plan = planTask(request.query.task);
      const auto effective = parseCommand(plan.command);
      if (effective.kind != CommandKind::kSample &&
          effective.kind != CommandKind::kCat) {
        throw std::runtime_error("Ask plan must resolve to sample or cat");
      }
      const auto table = catalog_.resolveTable(effective.path);
      auto askExplain = explainForResolvedCommand(
          clientMode_,
          effective,
          table,
          plan.reason,
          plan.goal,
          request.query.task,
          plan.command);
      askExplain.targetCommand = request.originalText;
      lastExplain_ = askExplain;
      const auto result = executeRequest(effective);
      lastExplain_ = askExplain;
      if (clientMode_ == ClientMode::kHuman) {
        return renderAskHuman(request.query.task, plan, result);
      }
      return renderAskAgent(request.query.task, plan, result);
    }
    case CommandKind::kExplain: {
      if (request.query.task == "last") {
        if (!lastExplain_.has_value()) {
          throw std::runtime_error("No previous command to explain");
        }
        return renderExplain(clientMode_, *lastExplain_);
      }
      const auto target = parseCommand(request.query.task);
      if (target.kind == CommandKind::kSample ||
          target.kind == CommandKind::kCat ||
          target.kind == CommandKind::kSchema ||
          target.kind == CommandKind::kAsk) {
        if (target.kind == CommandKind::kAsk) {
          const auto plan = planTask(target.query.task);
          const auto effective = parseCommand(plan.command);
          const auto table = catalog_.resolveTable(effective.path);
          auto explain = explainForResolvedCommand(
              clientMode_,
              effective,
              table,
              plan.reason,
              plan.goal,
              target.query.task,
              plan.command);
          explain.targetCommand = target.originalText;
          return renderExplain(clientMode_, explain);
        }
        const auto table = catalog_.resolveTable(target.path);
        return renderExplain(
            clientMode_,
            explainForResolvedCommand(
                clientMode_,
                target,
                table,
                target.kind == CommandKind::kSchema
                    ? "schema inspection keeps agent queries typed and bounded before execution"
                    : (target.kind == CommandKind::kSample
                           ? "bounded sampling limits scan scope before wider reads"
                           : "constrained filters, explicit metrics, and row limits keep access predictable")));
      }
      throw std::runtime_error(
          "Explain currently supports schema, sample, cat, ask, or last");
    }
  }
  throw std::runtime_error("Unsupported command");
}

} // namespace bytedance::bolt::tool::boltfs
