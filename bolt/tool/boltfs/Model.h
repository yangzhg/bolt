#pragma once

#include "bolt/common/memory/MemoryPool.h"
#include "bolt/core/PlanNode.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/exec/Task.h"
#include "bolt/vector/ComplexVector.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace bytedance::bolt::tool::boltfs {

inline constexpr const char* kScheme = "boltfs://";
inline constexpr const char* kWarehouse = "warehouse";
inline constexpr const char* kTpchBackend = "tpch";
inline constexpr const char* kDemoBackend = "demo";
inline constexpr uint64_t kDefaultSampleLimit = 5;
inline constexpr uint64_t kDefaultCatLimit = 20;
inline constexpr uint64_t kMaxResultRows = 200;

enum class CommandKind {
  kHelp,
  kLs,
  kCd,
  kPwd,
  kSchema,
  kSample,
  kCat,
  kAsk,
  kExplain,
  kExit,
};

enum class OutputFormat {
  kJson,
  kNdjson,
};

enum class ClientMode {
  kAgent,
  kHuman,
};

enum class BackendKind {
  kTpch,
  kDemo,
};

struct BoltFsPath {
  std::string raw;
  std::vector<std::string> segments;
};

struct QuerySpec {
  std::vector<std::string> columns;
  std::string filter;
  std::vector<std::string> groupBy;
  std::vector<std::string> metrics;
  std::string task;
  uint64_t limit{0};
  OutputFormat format{OutputFormat::kNdjson};
};

struct GuardrailInfo {
  std::string uri;
  std::string datasetBackend;
  std::string executionBackend;
  std::string clientMode;
  std::string outputFormat;
  std::string safetyReason;
  std::string filter;
  std::vector<std::string> columns;
  std::vector<std::string> groupBy;
  std::vector<std::string> metrics;
  uint64_t rowLimit{0};
};

struct ExplainInfo {
  std::string command;
  std::string targetCommand;
  std::string effectiveCommand;
  std::string task;
  std::string goal;
  std::string reason;
  GuardrailInfo guardrails;
};

struct CommandRequest {
  CommandKind kind;
  BoltFsPath path;
  QuerySpec query;
  std::string originalText;
};

struct CatalogEntry {
  std::string name;
  std::string kind;
  std::string uri;
};

struct QueryResult {
  RowTypePtr rowType;
  std::vector<RowVectorPtr> batches;
  std::shared_ptr<memory::MemoryPool> poolHolder;
  std::shared_ptr<core::QueryCtx> queryCtxHolder;
  std::shared_ptr<exec::Task> taskHolder;
  uint64_t rowCount{0};
  uint64_t limit{0};
  bool truncated{false};
  bool aggregated{false};
  OutputFormat format{OutputFormat::kNdjson};
  GuardrailInfo guardrails;
};

} // namespace bytedance::bolt::tool::boltfs
