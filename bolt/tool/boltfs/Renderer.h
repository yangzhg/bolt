#pragma once

#include "bolt/tool/boltfs/Model.h"

#include <string>
#include <vector>

namespace bytedance::bolt::tool::boltfs {

std::string renderLs(
    ClientMode clientMode,
    const BoltFsPath& path,
    const std::vector<CatalogEntry>& entries);

std::string renderSchema(
    ClientMode clientMode,
    const std::string& uri,
    const RowTypePtr& schema);

std::string renderSample(
    ClientMode clientMode,
    const std::string& uri,
    const QueryResult& result);

std::string renderCat(
    ClientMode clientMode,
    const std::string& uri,
    const QueryResult& result);

std::string renderExplain(ClientMode clientMode, const ExplainInfo& explain);

} // namespace bytedance::bolt::tool::boltfs
