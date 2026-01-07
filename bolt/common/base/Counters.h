/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <folly/Range.h>
namespace bytedance::bolt {

/// Bolt metrics Registration.
void registerBoltMetrics();

constexpr folly::StringPiece kMetricHiveFileHandleGenerateLatencyMs{
    "bolt.hive_file_handle_generate_latency_ms"};

constexpr folly::StringPiece kMetricCacheShrinkCount{"bolt.cache_shrink_count"};

constexpr folly::StringPiece kMetricCacheShrinkTimeMs{"bolt.cache_shrink_ms"};

constexpr folly::StringPiece kMetricMaxSpillLevelExceededCount{
    "bolt.spill_max_level_exceeded_count"};

constexpr folly::StringPiece kMetricQueryMemoryReclaimTimeMs{
    "bolt.query_memory_reclaim_time_ms"};

constexpr folly::StringPiece kMetricQueryMemoryReclaimedBytes{
    "bolt.query_memory_reclaim_bytes"};

constexpr folly::StringPiece kMetricQueryMemoryReclaimCount{
    "bolt.query_memory_reclaim_count"};

constexpr folly::StringPiece kMetricTaskMemoryReclaimCount{
    "bolt.task_memory_reclaim_count"};

constexpr folly::StringPiece kMetricTaskMemoryReclaimWaitTimeMs{
    "bolt.task_memory_reclaim_wait_ms"};

constexpr folly::StringPiece kMetricTaskMemoryReclaimExecTimeMs{
    "bolt.task_memory_reclaim_exec_ms"};

constexpr folly::StringPiece kMetricTaskMemoryReclaimWaitTimeoutCount{
    "bolt.task_memory_reclaim_wait_timeout_count"};

constexpr folly::StringPiece kMetricTaskSplitsCount{"bolt.task_splits_count"};

constexpr folly::StringPiece kMetricOpMemoryReclaimTimeMs{
    "bolt.op_memory_reclaim_time_ms"};

constexpr folly::StringPiece kMetricOpMemoryReclaimedBytes{
    "bolt.op_memory_reclaim_bytes"};

constexpr folly::StringPiece kMetricOpMemoryReclaimCount{
    "bolt.op_memory_reclaim_count"};

constexpr folly::StringPiece kMetricMemoryNonReclaimableCount{
    "bolt.memory_non_reclaimable_count"};

constexpr folly::StringPiece kMetricMemoryPoolInitialCapacityBytes{
    "bolt.memory_pool_initial_capacity_bytes"};

constexpr folly::StringPiece kMetricMemoryPoolCapacityGrowCount{
    "bolt.memory_pool_capacity_growth_count"};

constexpr folly::StringPiece kMetricMemoryPoolUsageLeakBytes{
    "bolt.memory_pool_usage_leak_bytes"};

constexpr folly::StringPiece kMetricMemoryPoolReservationLeakBytes{
    "bolt.memory_pool_reservation_leak_bytes"};

constexpr folly::StringPiece kMetricMemoryAllocatorDoubleFreeCount{
    "bolt.memory_allocator_double_free_count"};

constexpr folly::StringPiece kMetricArbitratorLocalArbitrationCount{
    "bolt.arbitrator_local_arbitration_count"};

constexpr folly::StringPiece kMetricArbitratorGlobalArbitrationCount{
    "bolt.arbitrator_global_arbitration_count"};

constexpr folly::StringPiece
    kMetricArbitratorGlobalArbitrationNumReclaimVictims{
        "bolt.arbitrator_global_arbitration_num_reclaim_victims"};

constexpr folly::StringPiece
    kMetricArbitratorGlobalArbitrationFailedVictimCount{
        "bolt.arbitrator_global_arbitration_failed_victim_count"};

constexpr folly::StringPiece kMetricArbitratorGlobalArbitrationBytes{
    "bolt.arbitrator_global_arbitration_bytes"};

constexpr folly::StringPiece kMetricArbitratorGlobalArbitrationTimeMs{
    "bolt.arbitrator_global_arbitration_time_ms"};

constexpr folly::StringPiece kMetricArbitratorGlobalArbitrationWaitCount{
    "bolt.arbitrator_global_arbitration_wait_count"};

constexpr folly::StringPiece kMetricArbitratorGlobalArbitrationWaitTimeMs{
    "bolt.arbitrator_global_arbitration_wait_time_ms"};

constexpr folly::StringPiece kMetricArbitratorAbortedCount{
    "bolt.arbitrator_aborted_count"};

constexpr folly::StringPiece kMetricArbitratorFailuresCount{
    "bolt.arbitrator_failures_count"};

constexpr folly::StringPiece kMetricArbitratorOpExecTimeMs{
    "bolt.arbitrator_op_exec_time_ms"};

constexpr folly::StringPiece kMetricArbitratorFreeCapacityBytes{
    "bolt.arbitrator_free_capacity_bytes"};

constexpr folly::StringPiece kMetricArbitratorFreeReservedCapacityBytes{
    "bolt.arbitrator_free_reserved_capacity_bytes"};

constexpr folly::StringPiece kMetricDriverYieldCount{"bolt.driver_yield_count"};

constexpr folly::StringPiece kMetricDriverQueueTimeMs{
    "bolt.driver_queue_time_ms"};

constexpr folly::StringPiece kMetricDriverExecTimeMs{
    "bolt.driver_exec_time_ms"};

constexpr folly::StringPiece kMetricSpilledInputBytes{"bolt.spill_input_bytes"};

constexpr folly::StringPiece kMetricSpilledBytes{"bolt.spill_bytes"};

constexpr folly::StringPiece kMetricSpilledRowsCount{"bolt.spill_rows_count"};

constexpr folly::StringPiece kMetricSpilledFilesCount{"bolt.spill_files_count"};

constexpr folly::StringPiece kMetricSpillFillTimeMs{"bolt.spill_fill_time_ms"};

constexpr folly::StringPiece kMetricSpillSortTimeMs{"bolt.spill_sort_time_ms"};

constexpr folly::StringPiece kMetricSpillExtractVectorTimeMs{
    "bolt.spill_extract_vector_time_ms"};

constexpr folly::StringPiece kMetricSpillSerializationTimeMs{
    "bolt.spill_serialization_time_ms"};

constexpr folly::StringPiece kMetricSpillWritesCount{"bolt.spill_writes_count"};

constexpr folly::StringPiece kMetricSpillFlushTimeMs{
    "bolt.spill_flush_time_ms"};

constexpr folly::StringPiece kMetricSpillWriteTimeMs{
    "bolt.spill_write_time_ms"};

constexpr folly::StringPiece kMetricSpillMemoryBytes{"bolt.spill_memory_bytes"};

constexpr folly::StringPiece kMetricSpillPeakMemoryBytes{
    "bolt.spill_peak_memory_bytes"};

constexpr folly::StringPiece kMetricFileWriterEarlyFlushedRawBytes{
    "bolt.file_writer_early_flushed_raw_bytes"};

constexpr folly::StringPiece kMetricHiveSortWriterFinishTimeMs{
    "bolt.hive_sort_writer_finish_time_ms"};

constexpr folly::StringPiece kMetricArbitratorRequestsCount{
    "bolt.arbitrator_requests_count"};

constexpr folly::StringPiece kMetricMemoryAllocatorMappedBytes{
    "bolt.memory_allocator_mapped_bytes"};

constexpr folly::StringPiece kMetricMemoryAllocatorExternalMappedBytes{
    "bolt.memory_allocator_external_mapped_bytes"};

constexpr folly::StringPiece kMetricMemoryAllocatorAllocatedBytes{
    "bolt.memory_allocator_allocated_bytes"};

constexpr folly::StringPiece kMetricMemoryAllocatorTotalUsedBytes{
    "bolt.memory_allocator_total_used_bytes"};

constexpr folly::StringPiece kMetricMmapAllocatorDelegatedAllocatedBytes{
    "bolt.mmap_allocator_delegated_allocated_bytes"};

constexpr folly::StringPiece kMetricCacheMaxAgeSecs{"bolt.cache_max_age_secs"};

constexpr folly::StringPiece kMetricMemoryCacheNumTinyEntries{
    "bolt.memory_cache_num_tiny_entries"};

constexpr folly::StringPiece kMetricMemoryCacheNumLargeEntries{
    "bolt.memory_cache_num_large_entries"};

constexpr folly::StringPiece kMetricMemoryCacheNumEmptyEntries{
    "bolt.memory_cache_num_empty_entries"};

constexpr folly::StringPiece kMetricMemoryCacheNumSharedEntries{
    "bolt.memory_cache_num_shared_entries"};

constexpr folly::StringPiece kMetricMemoryCacheNumExclusiveEntries{
    "bolt.memory_cache_num_exclusive_entries"};

constexpr folly::StringPiece kMetricMemoryCacheNumPrefetchedEntries{
    "bolt.memory_cache_num_prefetched_entries"};

constexpr folly::StringPiece kMetricMemoryCacheTotalTinyBytes{
    "bolt.memory_cache_total_tiny_bytes"};

constexpr folly::StringPiece kMetricMemoryCacheTotalLargeBytes{
    "bolt.memory_cache_total_large_bytes"};

constexpr folly::StringPiece kMetricMemoryCacheTotalTinyPaddingBytes{
    "bolt.memory_cache_total_tiny_padding_bytes"};

constexpr folly::StringPiece kMetricMemoryCacheTotalLargePaddingBytes{
    "bolt.memory_cache_total_large_padding_bytes"};

constexpr folly::StringPiece kMetricMemoryCacheTotalPrefetchBytes{
    "bolt.memory_cache_total_prefetched_bytes"};

constexpr folly::StringPiece kMetricMemoryCacheSumEvictScore{
    "bolt.memory_cache_sum_evict_score"};

constexpr folly::StringPiece kMetricMemoryCacheNumHits{
    "bolt.memory_cache_num_hits"};

constexpr folly::StringPiece kMetricMemoryCacheHitBytes{
    "bolt.memory_cache_hit_bytes"};

constexpr folly::StringPiece kMetricMemoryCacheNumNew{
    "bolt.memory_cache_num_new"};

constexpr folly::StringPiece kMetricMemoryCacheNumEvicts{
    "bolt.memory_cache_num_evicts"};

constexpr folly::StringPiece kMetricMemoryCacheNumSavableEvicts{
    "bolt.memory_cache_num_savable_evicts"};

constexpr folly::StringPiece kMetricMemoryCacheNumEvictChecks{
    "bolt.memory_cache_num_evict_checks"};

constexpr folly::StringPiece kMetricMemoryCacheNumWaitExclusive{
    "bolt.memory_cache_num_wait_exclusive"};

constexpr folly::StringPiece kMetricMemoryCacheNumAllocClocks{
    "bolt.memory_cache_num_alloc_clocks"};

constexpr folly::StringPiece kMetricMemoryCacheNumAgedOutEntries{
    "bolt.memory_cache_num_aged_out_entries"};

constexpr folly::StringPiece kMetricMemoryCacheNumStaleEntries{
    "bolt.memory_cache_num_stale_entries"};

constexpr folly::StringPiece kMetricSsdCacheCachedRegions{
    "bolt.ssd_cache_cached_regions"};

constexpr folly::StringPiece kMetricSsdCacheCachedEntries{
    "bolt.ssd_cache_cached_entries"};

constexpr folly::StringPiece kMetricSsdCacheCachedBytes{
    "bolt.ssd_cache_cached_bytes"};

constexpr folly::StringPiece kMetricSsdCacheReadEntries{
    "bolt.ssd_cache_read_entries"};

constexpr folly::StringPiece kMetricSsdCacheReadBytes{
    "bolt.ssd_cache_read_bytes"};

constexpr folly::StringPiece kMetricSsdCacheWrittenEntries{
    "bolt.ssd_cache_written_entries"};

constexpr folly::StringPiece kMetricSsdCacheWrittenBytes{
    "bolt.ssd_cache_written_bytes"};

constexpr folly::StringPiece kMetricSsdCacheAgedOutEntries{
    "bolt.ssd_cache_aged_out_entries"};

constexpr folly::StringPiece kMetricSsdCacheAgedOutRegions{
    "bolt.ssd_cache_aged_out_regions"};

constexpr folly::StringPiece kMetricSsdCacheOpenSsdErrors{
    "bolt.ssd_cache_open_ssd_errors"};

constexpr folly::StringPiece kMetricSsdCacheOpenCheckpointErrors{
    "bolt.ssd_cache_open_checkpoint_errors"};

constexpr folly::StringPiece kMetricSsdCacheOpenLogErrors{
    "bolt.ssd_cache_open_log_errors"};

constexpr folly::StringPiece kMetricSsdCacheMetaFileDeleteErrors{
    "bolt.ssd_cache_delete_meta_file_errors"};

constexpr folly::StringPiece kMetricSsdCacheGrowFileErrors{
    "bolt.ssd_cache_grow_file_errors"};

constexpr folly::StringPiece kMetricSsdCacheWriteSsdErrors{
    "bolt.ssd_cache_write_ssd_errors"};

constexpr folly::StringPiece kMetricSsdCacheWriteSsdDropped{
    "bolt.ssd_cache_write_ssd_dropped"};

constexpr folly::StringPiece kMetricSsdCacheWriteCheckpointErrors{
    "bolt.ssd_cache_write_checkpoint_errors"};

constexpr folly::StringPiece kMetricSsdCacheReadCorruptions{
    "bolt.ssd_cache_read_corruptions"};

constexpr folly::StringPiece kMetricSsdCacheReadSsdErrors{
    "bolt.ssd_cache_read_ssd_errors"};

constexpr folly::StringPiece kMetricSsdCacheReadCheckpointErrors{
    "bolt.ssd_cache_read_checkpoint_errors"};

constexpr folly::StringPiece kMetricSsdCacheReadWithoutChecksum{
    "bolt.ssd_cache_read_without_checksum"};

constexpr folly::StringPiece kMetricSsdCacheCheckpointsRead{
    "bolt.ssd_cache_checkpoints_read"};

constexpr folly::StringPiece kMetricSsdCacheCheckpointsWritten{
    "bolt.ssd_cache_checkpoints_written"};

constexpr folly::StringPiece kMetricSsdCacheRegionsEvicted{
    "bolt.ssd_cache_regions_evicted"};

constexpr folly::StringPiece kMetricSsdCacheRecoveredEntries{
    "bolt.ssd_cache_recovered_entries"};

constexpr folly::StringPiece kMetricExchangeTransactionCreateDelay{
    "bolt.exchange.transaction_create_delay_ms"};

constexpr folly::StringPiece kMetricExchangeDataTimeMs{
    "bolt.exchange_data_time_ms"};

constexpr folly::StringPiece kMetricExchangeDataBytes{
    "bolt.exchange_data_bytes"};

constexpr folly::StringPiece kMetricExchangeDataSize{"bolt.exchange_data_size"};

constexpr folly::StringPiece kMetricExchangeDataCount{
    "bolt.exchange_data_count"};

constexpr folly::StringPiece kMetricExchangeDataSizeTimeMs{
    "bolt.exchange_data_size_time_ms"};

constexpr folly::StringPiece kMetricExchangeDataSizeCount{
    "bolt.exchange_data_size_count"};

constexpr folly::StringPiece kMetricStorageThrottledDurationMs{
    "bolt.storage_throttled_duration_ms"};

constexpr folly::StringPiece kMetricStorageLocalThrottled{
    "bolt.storage_local_throttled_count"};

constexpr folly::StringPiece kMetricStorageGlobalThrottled{
    "bolt.storage_global_throttled_count"};

constexpr folly::StringPiece kMetricStorageNetworkThrottled{
    "bolt.storage_network_throttled_count"};

constexpr folly::StringPiece kMetricIndexLookupResultRawBytes{
    "bolt.index_lookup_result_raw_bytes"};

constexpr folly::StringPiece kMetricIndexLookupResultBytes{
    "bolt.index_lookup_result_bytes"};

constexpr folly::StringPiece kMetricIndexLookupTimeMs{
    "bolt.index_lookup_time_ms"};

constexpr folly::StringPiece kMetricIndexLookupWaitTimeMs{
    "bolt.index_lookup_wait_time_ms"};

constexpr folly::StringPiece kMetricIndexLookupBlockedWaitTimeMs{
    "bolt.index_lookup_blocked_wait_time_ms"};

constexpr folly::StringPiece kMetricIndexLookupErrorResultCount{
    "bolt.index_lookup_error_result_count"};

constexpr folly::StringPiece kMetricTableScanBatchProcessTimeMs{
    "bolt.table_scan_batch_process_time_ms"};

constexpr folly::StringPiece kMetricTableScanBatchBytes{
    "bolt.table_scan_batch_bytes"};

constexpr folly::StringPiece kMetricTaskBatchProcessTimeMs{
    "bolt.task_batch_process_time_ms"};

constexpr folly::StringPiece kMetricTaskBarrierProcessTimeMs{
    "bolt.task_barrier_process_time_ms"};

constexpr folly::StringPiece kUDFCall{"bolt.udf.call"};

constexpr folly::StringPiece kUDFCallError{"bolt.udf.call.error"};

constexpr folly::StringPiece kUDFCallTimeMs{"bolt.udf.call.time.ms"};

constexpr folly::StringPiece kMetricSpillTotalTimeMs{
    "bolt.spill_total_time_ms"};

constexpr folly::StringPiece kMetricSpillConvertTimeMs{
    "bolt.spill_convert_time_ms"};

constexpr folly::StringPiece kMetricReadFileSize0MB{
    "bolt.read_file_size_0_1mb"};

constexpr folly::StringPiece kMetricReadFileSize8MB{
    "bolt.read_file_size_1_8mb"};

constexpr folly::StringPiece kMetricReadFileSize16MB{
    "bolt.read_file_size_8_16mb"};

constexpr folly::StringPiece kMetricReadFileSize32MB{
    "bolt.read_file_size_16_32mb"};

constexpr folly::StringPiece kMetricReadFileSizeLarge{
    "bolt.read_file_size_large"};

} // namespace bytedance::bolt
