#pragma once

#include "bolt/tool/boltfs/Model.h"

#include <string>

namespace bytedance::bolt::tool::boltfs {

CommandRequest parseCommand(std::string_view text);

std::string helpText();

} // namespace bytedance::bolt::tool::boltfs
