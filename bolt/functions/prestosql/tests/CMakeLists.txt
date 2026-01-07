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
  bolt_functions_test
  ArithmeticTest.cpp
  ArrayAllMatchTest.cpp
  ArrayAnyMatchTest.cpp
  ArrayAverageTest.cpp
  ArrayCombinationsTest.cpp
  ArrayConcatTest.cpp
  ArrayConstructorTest.cpp
  ArrayContainsTest.cpp
  ArrayDistinctTest.cpp
  ArrayDuplicatesTest.cpp
  ArrayExceptTest.cpp
  ArrayFilterTest.cpp
  ArrayFrequencyTest.cpp
  ArrayFlattenTest.cpp
  ArrayJoinTest.cpp
  ArrayHasDuplicatesTest.cpp
  ArrayIntersectTest.cpp
  ArrayMaxTest.cpp
  ArrayMinTest.cpp
  ArrayNGramsTest.cpp
  ArrayNoneMatchTest.cpp
  ArrayNormalizeTest.cpp
  ArrayPositionTest.cpp
  ArrayRemoveTest.cpp
  ArraySortTest.cpp
  ArrayShuffleTest.cpp
  ArraysOverlapTest.cpp
  ArraySumTest.cpp
  ArrayTrimTest.cpp
  ArrayUnionTest.cpp
  BinaryFunctionsTest.cpp
  BitwiseTest.cpp
  CardinalityTest.cpp
  CeilFloorTest.cpp
  ComparisonsTest.cpp
  PrestoSqlDateTimeFunctionsTest.cpp
  DecimalArithmeticTest.cpp
  ElementTest.cpp
  ElementAtTest.cpp
  FindFirstTest.cpp
  FromUtf8Test.cpp
  HyperLogLogCastTest.cpp
  HyperLogLogFunctionsTest.cpp
  InPredicateTest.cpp
  JsonCastTest.cpp
  JsonExtractScalarTest.cpp
  JsonExtractScalarWithDetectedTest.cpp
  JsonFunctionsTest.cpp
  MapEntriesTest.cpp
  MapFilterTest.cpp
  MapFromEntriesTest.cpp
  MultimapFromEntriesTest.cpp
  MapKeysAndValuesTest.cpp
  MapMatchTest.cpp
  MapTest.cpp
  MapZipWithTest.cpp
  NotTest.cpp
  ProbabilityTest.cpp
  RandTest.cpp
  ReduceTest.cpp
  RegexpFunctionsTest.cpp
  ReverseTest.cpp
  RoundTest.cpp
  ScalarFunctionRegTest.cpp
  SliceTest.cpp
  SequenceTest.cpp
  SplitTest.cpp
  SplitPartTest.cpp
  SplitToMapTest.cpp
  StringFunctionsTest.cpp
  StringToMapTest.cpp
  TimestampWithTimeZoneCastTest.cpp
  ToJsonTest.cpp
  TransformTest.cpp
  TransformKeysTest.cpp
  TransformValuesTest.cpp
  TrimFunctionsTest.cpp
  URLFunctionsTest.cpp
  Utf8Test.cpp
  WidthBucketArrayTest.cpp
  GreatestLeastTest.cpp
  ZipTest.cpp
  ZipWithTest.cpp
  DownsampleFunctionTest.cpp)

add_test(bolt_functions_test bolt_functions_test)

target_link_libraries(
  bolt_functions_test
  bolt_functions_test_lib
  bolt_exec
  bolt_expression
  bolt_functions_prestosql
  bolt_functions_lib
  bolt_exec_test_lib
  bolt_dwio_common_test_utils
  bolt_vector_fuzzer
  GTest::gtest
  GTest::gtest_main
  gflags::gflags
  GTest::gmock
  GTest::gmock_main)

add_executable(bolt_functions_simple_test SimpleComparisonMatcherTest.cpp)

add_test(bolt_functions_simple_test bolt_functions_simple_test)

target_link_libraries(bolt_functions_simple_test bolt_functions_test_lib
                      GTest::gtest GTest::gtest_main)
