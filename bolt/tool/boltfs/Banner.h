#pragma once

#include <string>

namespace bytedance::bolt::tool::boltfs {

std::string welcomeBanner();
bool shouldShowWelcomeBanner(
    bool interactiveStdin,
    bool interactiveStdout,
    bool hasCommandArgs);

} // namespace bytedance::bolt::tool::boltfs
