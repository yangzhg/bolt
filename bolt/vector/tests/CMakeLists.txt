# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# --------------------------------------------------------------------------
# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0
#
# This file has been modified by ByteDance Ltd. and/or its affiliates on
# 2025-11-11.
#
# Original file was released under the Apache License 2.0,
# with the full license text available at:
#     http://www.apache.org/licenses/LICENSE-2.0
#
# This modified file is released under the same license.
# --------------------------------------------------------------------------

add_subdirectory(utils)

add_executable(
  bolt_vector_test
  VectorCompareTest.cpp
  VectorSaverTest.cpp
  VectorMakerTest.cpp
  VectorPoolTest.cpp
  VectorPrinterTest.cpp
  VectorTest.cpp
  VectorStreamTest.cpp
  VectorToStringTest.cpp
  VectorEstimateFlatSizeTest.cpp
  VectorPrepareForReuseTest.cpp
  DecodedVectorTest.cpp
  SelectivityVectorTest.cpp
  EnsureWritableVectorTest.cpp
  IsWritableVectorTest.cpp
  LazyVectorTest.cpp
  MayHaveNullsRecursiveTest.cpp
  VariantToVectorTest.cpp
  VectorTestUtils.cpp)

add_test(bolt_vector_test bolt_vector_test)

target_link_libraries(
  bolt_vector_test
  bolt_vector_test_lib
  bolt_buffer
  bolt_vector
  bolt_serialization
  bolt_memory
  bolt_presto_serializer
  bolt_presto_types
  bolt_temp_path
  bolt_vector_fuzzer
  Boost::atomic
  Boost::context
  Boost::date_time
  Boost::filesystem
  Boost::program_options
  Boost::thread
  Boost::system
  GTest::gtest
  GTest::gtest_main
  ${FOLLY_WITH_DEPENDENCIES}
  utf8proc::utf8proc
  gflags::gflags
  glog::glog
  fmt::fmt
  dl)

add_executable(simple_vector_test SimpleVectorTest.cpp ToStringUtilityTest.cpp)

add_test(simple_vector_test simple_vector_test)

target_link_libraries(
  simple_vector_test
  bolt_vector
  bolt_buffer
  bolt_type
  bolt_vector_test_lib
  GTest::gtest
  GTest::gtest_main
  ${FOLLY_WITH_DEPENDENCIES}
  gflags::gflags
  glog::glog
  fmt::fmt)

add_executable(bias_vector_test BiasVectorTest.cpp)

add_test(bias_vector_test bias_vector_test)

target_link_libraries(
  bias_vector_test
  bolt_vector
  Boost::filesystem
  GTest::gtest
  GTest::gtest_main
  ${FOLLY_WITH_DEPENDENCIES}
  gflags::gflags
  glog::glog
  fmt::fmt)
