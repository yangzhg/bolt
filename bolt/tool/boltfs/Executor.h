#pragma once

#include "bolt/tool/boltfs/Catalog.h"
#include "bolt/tool/boltfs/Model.h"

namespace bytedance::bolt::tool::boltfs {

class Executor {
 public:
  Executor();

  QueryResult sample(const ResolvedTable& table, const QuerySpec& query) const;

  QueryResult cat(const ResolvedTable& table, const QuerySpec& query) const;
};

} // namespace bytedance::bolt::tool::boltfs
