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


add_library(
  bolt_query_trace_replayer_base
  AggregationReplayer.cpp
  FilterProjectReplayer.cpp
  HashJoinReplayer.cpp
  OperatorReplayerBase.cpp
  PartitionedOutputReplayer.cpp
  TableScanReplayer.cpp
  TableWriterReplayer.cpp
  TraceReplayRunner.cpp
  TraceReplayTaskRunner.cpp)

set(enabled_filesystems "")

if(BOLT_ENABLE_S3)
    list(APPEND enabled_filesystems "bolt_s3fs")
endif()
if(BOLT_ENABLE_GCS)
    list(APPEND enabled_filesystems "bolt_gcs")
endif()
if(BOLT_ENABLE_HDFS)
    list(APPEND enabled_filesystems "bolt_hdfs")
endif()
if(BOLT_ENABLE_ABFS)
    list(APPEND enabled_filesystems "bolt_abfs")
endif()

set(enabled_fileformats "")


if(BOLT_ENABLE_PARQUET)
    list(APPEND enabled_fileformats "bolt_dwio_parquet_reader")
    list(APPEND enabled_fileformats "bolt_dwio_parquet_writer")
endif()
if(BOLT_ENABLE_ORC)
    list(APPEND enabled_fileformats "bolt_dwio_orc_reader")
    list(APPEND enabled_fileformats "bolt_dwio_orc_writer")
endif()

target_link_libraries(
  bolt_query_trace_replayer_base
  bolt_aggregates
  bolt_type
  bolt_vector
  bolt_vector_test_lib
  bolt_exec
  bolt_exec_test_lib
  ${enabled_fileformats}
  bolt_dwio_parquet_reader
  bolt_dwio_parquet_writer
  bolt_hive_connector
  bolt_tpch_connector
  bolt_memory
  ${enabled_filesystems}
  Folly::folly
  glog::glog
  gflags::gflags)

add_executable(bolt_query_replayer TraceReplayerMain.cpp)

target_link_libraries(
  bolt_query_replayer
  bolt_query_trace_replayer_base
  bolt_exec
  bolt_exec_test_lib
  bolt_tpch_connector)

add_library(bolt_trace_file_tool_base TraceFileToolRunner.cpp)

target_link_libraries(
  bolt_trace_file_tool_base
  bolt_exec
  bolt_type
  bolt_vector
  bolt_exec
  bolt_hive_connector
  Folly::folly
  glog::glog
  gflags::gflags)

add_executable(bolt_trace_file_tool TraceFileToolMain.cpp)

target_link_libraries(
  bolt_trace_file_tool bolt_trace_file_tool_base)

add_subdirectory(tests)
