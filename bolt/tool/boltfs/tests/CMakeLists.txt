add_executable(
  bolt_boltfs_test
  Main.cpp
  BoltFsTest.cpp
  CatalogTest.cpp
  ExecutorTest.cpp
  ParserTest.cpp
)

target_link_libraries(
  bolt_boltfs_test
  PRIVATE
  bolt_boltfs
  GTest::gtest
)

add_test(
  NAME bolt_boltfs_test
  COMMAND bolt_boltfs_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
