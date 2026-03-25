#pragma once

#include "bolt/tool/boltfs/Model.h"
#include "bolt/tpch/gen/TpchGen.h"

#include <vector>

namespace bytedance::bolt::tool::boltfs {

struct ResolvedTable {
  std::string uri;
  BackendKind backend{BackendKind::kTpch};
  tpch::Table table;
  std::string tableName;
  RowTypePtr schema;
};

class Catalog {
 public:
  std::vector<CatalogEntry> list(const BoltFsPath& path) const;

  ResolvedTable resolveTable(const BoltFsPath& path) const;

  RowTypePtr schema(const BoltFsPath& path) const;
};

} // namespace bytedance::bolt::tool::boltfs
