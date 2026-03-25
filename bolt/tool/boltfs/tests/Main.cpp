#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  const auto result = RUN_ALL_TESTS();
  std::fflush(stdout);
  std::fflush(stderr);
  std::_Exit(result);
}
