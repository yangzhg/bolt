add_library(
  bolt_boltfs
  Banner.cpp
  BoltFs.cpp
  Catalog.cpp
  Executor.cpp
  Parser.cpp
  Renderer.cpp
)

target_link_libraries(
  bolt_boltfs
  PUBLIC
  bolt_exec
  bolt_exec_test_lib
  bolt_tpch_connector
  bolt_parse_parser
  bolt_functions_prestosql
  bolt_aggregates
  bolt_testutils
  Folly::folly
  fmt::fmt
)

add_executable(boltfs BoltFsMain.cpp)

find_package(readline CONFIG REQUIRED)

target_link_libraries(
  boltfs
  PRIVATE
  bolt_boltfs
  readline::readline
)
target_compile_definitions(
  boltfs
  PRIVATE
  BOLTFS_HAS_READLINE=1
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
