#pragma once

#include "bolt/tool/boltfs/Catalog.h"
#include "bolt/tool/boltfs/Executor.h"

#include <optional>
#include <string>
#include <vector>

namespace bytedance::bolt::tool::boltfs {

class BoltFs {
 public:
  explicit BoltFs(ClientMode clientMode = ClientMode::kAgent);

  std::string execute(std::string_view commandLine) const;
  std::vector<std::string> completeCommand(std::string_view prefix) const;
  std::vector<std::string> completePath(std::string_view prefix) const;

 private:
  std::string executeRequest(const CommandRequest& request) const;
  BoltFsPath resolvePath(const BoltFsPath& path) const;

  ClientMode clientMode_;
  Catalog catalog_;
  Executor executor_;
  mutable std::optional<ExplainInfo> lastExplain_;
  mutable BoltFsPath cwd_;
};

} // namespace bytedance::bolt::tool::boltfs
