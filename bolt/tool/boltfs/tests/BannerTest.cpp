#include "bolt/tool/boltfs/Banner.h"

#include <gtest/gtest.h>

namespace bytedance::bolt::tool::boltfs {
namespace {

TEST(BannerTest, ShowsBannerOnlyForInteractiveRepl) {
  EXPECT_TRUE(shouldShowWelcomeBanner(true, true, false));
  EXPECT_FALSE(shouldShowWelcomeBanner(false, true, false));
  EXPECT_FALSE(shouldShowWelcomeBanner(true, false, false));
  EXPECT_FALSE(shouldShowWelcomeBanner(true, true, true));
}

} // namespace
} // namespace bytedance::bolt::tool::boltfs
