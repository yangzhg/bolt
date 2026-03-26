#include "bolt/tool/boltfs/Banner.h"

namespace bytedance::bolt::tool::boltfs {

std::string welcomeBanner() {
  return R"BOLTFS(
 ____   ___   _      _____ _____ ____
| __ ) / _ \ | |    |_   _|  ___/ ___|
|  _ \| | | || |      | | | |_  \___ \
| |_) | |_| || |___   | | |  _|  ___) |
|____/ \___/ |_____|  |_| |_|   |____/

Filesystem-style data access powered by Bolt
Type help to see supported commands.
)BOLTFS";
}

bool shouldShowWelcomeBanner(
    bool interactiveStdin,
    bool interactiveStdout,
    bool hasCommandArgs) {
  return interactiveStdin && interactiveStdout && !hasCommandArgs;
}

} // namespace bytedance::bolt::tool::boltfs
