/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.trino;

import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.WriteMode;

import java.util.List;
import java.util.Map;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.doubleProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;

/** Trino table options */
public class TrinoTableOptions {

    public static final String FILE_FORMAT_PROPERTY = "file_format";
    public static final String PRIMARY_KEY_IDENTIFIER = "primary_key";
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";
    public static final String BUCKET = "bucket";
    public static final String BUCKET_KEY = "bucket_key";
    public static final String CHANGELOG_PRODUCER = "changelog_producer";
    public static final String COMMIT_FORCE_COMPACT = "commit_force_compact";
    public static final String MERGE_ENGINE = "merge_engine";
    public static final String COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT = "compaction_max_size_amplification_percent";
    public static final String COMPACTION_MAX_SORTED_RUN_NUM = "compaction_max_sorted_run_num";
    public static final String COMPACTION_MAX_FILE_NUM = "compaction_max_file_num";
    public static final String COMPACTION_MIN_FILE_NUM = "compaction_min_file_num";
    public static final String COMPACTION_SIZE_RATIO = "compaction_size_ratio";
    public static final String CONTINUOUS_DISCOVERY_INTERVAL = "continuous_discovery_interval";
    public static final String DYNAMIC_PARTITION_OVERWRITE = "dynamic_partition_overwrite";
    public static final String FULL_COMPACTION_DELTA_COMMITS = "full_compaction_delta_commits";
    public static final String LOG_CHANGELOG_MODE = "log_changelog_mode";
    public static final String LOG_CONSISTENCY = "log_consistency";
    public static final String LOG_FORMAT = "log_format";
    public static final String LOG_SCAN_REMOVE_NORMALIZE = "log_scan_remove_normalize";
    public static final String LOOKUP_CACHE_FILE_RETENTION = "lookup_cache_file-retention";
    public static final String LOOKUP_CACHE_MAX_DISK_SIZE = "lookup_cache_max_disk_size";
    public static final String LOOKUP_CACHE_MAX_MEMORY_SIZE = "lookup_cache_max_memory_size";
    public static final String LOOKUP_HASH_LOAD_FACTOR = "lookup_hash_load_factor";
    public static final String MANIFEST_FORMAT = "manifest_format";
    public static final String MANIFEST_MERGE_MIN_COUNT = "manifest_merge_min_count";
    public static final String MANIFEST_TARGET_FILE_SIZE = "manifest_target_file_size";
    public static final String NUM_LEVELS = "num_levels";
    public static final String NUM_SORTED_RUNS_COMPACTION_TRIGGER = "num_sorted_run_compaction_trigger";
    public static final String NUM_SORTED_RUNS_STOP_TRIGGER = "num_sorted_run_stop_trigger";
    public static final String ORC_BLOOM_FILTER_COLUMNS = "orc_bloom_filter_columns";
    public static final String ORC_BLOOM_FILTER_FPP = "orc_bloom_filter_fpp";
    public static final String PAGE_SIZE = "page_size";
    public static final String PARTIAL_UPDATE_IGNORE_DELETE = "partial_update_ignore_delete";
    public static final String PARTITION_EXPIRATION_CHECK_INTERVAL = "partition_expiration_check_interval";
    public static final String PARTITION_EXPIRATION_TIME = "partition_expiration_time";
    public static final String PARTITION_TIMESTAMP_FORMATTER = "partition_timestamp_formatter";
    public static final String PARTITION_TIMESTAMP_PATTERN = "partition_timestamp_pattern";
    public static final String READ_BATCH_SIZE = "read_batch_size";
    public static final String SCAN_BOUNDED_WATERMARK = "scan_bounded_watermark";
    public static final String SNAPSHOT_NUM_RETAINED_MIN = "snapshot_num_retained_min";
    public static final String SNAPSHOT_NUM_RETAINED_MAX = "snapshot_num_retained_max";
    public static final String SNAPSHOT_TIME_RETAINED = "snapshot_time_retained";
    public static final String SOURCE_SPLIT_OPEN_FILE_COST = "source_split_open_file_cost";
    public static final String SOURCE_SPLIT_TARGET_SIZE = "source_split_target_size";
    public static final String STREAMING_READ_OVERWRITE = "streaming_read_overwrite";
    public static final String TARGET_FILE_SIZE = "target_file_size";
    public static final String WRITE_BUFFER_SIZE = "write_buffer_size";
    public static final String WRITE_BUFFER_SPILLABLE = "write_buffer_spillable";
    public static final String WRITE_MODE = "write_mode";
    public static final String WRITE_ONLY = "write_only";

    private final List<PropertyMetadata<?>> tableProperties;

    public TrinoTableOptions() {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
            .add(enumProperty(
                FILE_FORMAT_PROPERTY,
                "File format for the table.",
                CoreOptions.FileFormatType.class,
                null,
                false
            ))
            .add(new PropertyMetadata<>(
                PRIMARY_KEY_IDENTIFIER,
                "Primary keys for the table.",
                new ArrayType(VARCHAR),
                List.class,
                ImmutableList.of(),
                false,
                value -> (List<?>) value,
                value -> value))
            .add(new PropertyMetadata<>(
                PARTITIONED_BY_PROPERTY,
                "Partition keys for the table.",
                new ArrayType(VARCHAR),
                List.class,
                ImmutableList.of(),
                false,
                value -> (List<?>) value,
                value -> value))
            .add(integerProperty(
                BUCKET,
                "Bucket number for the table.",
                null,
                false
            ))
            .add(stringProperty(
                BUCKET_KEY,
                "Bucket key for the table.",
                null,
                false
            ))
            .add(enumProperty(
                CHANGELOG_PRODUCER,
                "Changelog producer for the table.",
                CoreOptions.ChangelogProducer.class,
                null,
                false
            ))
            .add(booleanProperty(
                COMMIT_FORCE_COMPACT,
                "Whether to force a compaction before commit.",
                null,
                false
            ))
            .add(enumProperty(
                MERGE_ENGINE,
                "Specify the merge engine for table with primary key.",
                CoreOptions.MergeEngine.class,
                null,
                false
            ))
            .add(integerProperty(
                COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT,
                "he size amplification is defined as the amount (in percentage) of additional storage " +
                    "needed to store a single byte of data in the merge tree for changelog mode table.",
                null,
                false
            ))
            .add(integerProperty(
                COMPACTION_MAX_SORTED_RUN_NUM,
                "The maximum sorted run number to pick for compaction. " +
                    "This value avoids merging too much sorted runs at the same time during compaction," +
                    "which may lead to OutOfMemoryError.",
                null,
                false
            ))
            .add(integerProperty(
                COMPACTION_MAX_FILE_NUM,
                "For file set [f_0,...,f_N], the maximum file number to trigger a compaction " +
                    "for append-only table, even if sum(size(f_i)) < targetFileSize. This value" +
                    "avoids pending too much small files, which slows down the performance.",
                null,
                false
            ))
            .add(integerProperty(
                COMPACTION_MIN_FILE_NUM,
                "For file set [f_0,...,f_N], the minimum file number which satisfies " +
                    "sum(size(f_i)) >= targetFileSize to trigger a compaction for " +
                    "append-only table. This value avoids almost-full-file to be compacted," +
                    "which is not cost-effective.",
                null,
                false
            ))
            .add(integerProperty(
                COMPACTION_SIZE_RATIO,
                "Percentage flexibility while comparing sorted run size for changelog mode table. If the candidate sorted run(s) " +
                    "size is 1% smaller than the next sorted run's size, then include next sorted run " +
                    "into this candidate set.",
                null,
                false
            ))
            .add(stringProperty(
                CONTINUOUS_DISCOVERY_INTERVAL,
                "he discovery interval of continuous reading.",
                null,
                false
            ))
            .add(booleanProperty(
                DYNAMIC_PARTITION_OVERWRITE,
                "Whether only overwrite dynamic partition when overwriting a partitioned table with " +
                    "dynamic partition columns. Works only when the table has partition keys.",
                null,
                false
            ))
            .add(integerProperty(
                FULL_COMPACTION_DELTA_COMMITS,
                "Full compaction will be constantly triggered after delta commits.",
                null,
                false
            ))
            .add(enumProperty(
                LOG_CHANGELOG_MODE,
                "Specify the log changelog mode for table.",
                CoreOptions.LogChangelogMode.class,
                null,
                false
            ))
            .add(enumProperty(
                LOG_CONSISTENCY,
                "Specify the log consistency mode for table.",
                CoreOptions.LogChangelogMode.class,
                null,
                false
            ))
            .add(stringProperty(
                LOG_FORMAT,
                "Specify the message format of log system.",
                null,
                false
            ))
            .add(booleanProperty(
                LOG_SCAN_REMOVE_NORMALIZE,
                "Whether to force the removal of the normalize node when streaming read." +
                    " Note: This is dangerous and is likely to cause data errors if downstream" +
                    "is used to calculate aggregation and the input is not complete changelog.",
                null,
                false
            ))
            .add(stringProperty(
                LOOKUP_CACHE_FILE_RETENTION,
                "The cached files retention time for lookup. After the file expires," +
                    " if there is a need for access, it will be re-read from the DFS to build" +
                    " an index on the local disk.",
                null,
                false
            ))
            .add(stringProperty(
                LOOKUP_CACHE_MAX_DISK_SIZE,
                "Max disk size for lookup cache, you can use this option to limit the use of local disks.",
                null,
                false
            ))
            .add(stringProperty(
                LOOKUP_CACHE_MAX_MEMORY_SIZE,
                "Max memory size for lookup cache.",
                null,
                false
            ))
            .add(doubleProperty(
                LOOKUP_HASH_LOAD_FACTOR,
                "The index load factor for lookup.",
                null,
                false
            ))
            .add(enumProperty(
                MANIFEST_FORMAT,
                "Specify the message format of manifest files.",
                CoreOptions.FileFormatType.class,
                null,
                false
            ))
            .add(integerProperty(
                MANIFEST_MERGE_MIN_COUNT,
                "To avoid frequent manifest merges, this parameter specifies the minimum number " +
                    "of ManifestFileMeta to merge.",
                null,
                false
            ))
            .add(stringProperty(
                MANIFEST_TARGET_FILE_SIZE,
                "Suggested file size of a manifest file.",
                null,
                false
            ))
            .add(integerProperty(
                NUM_LEVELS,
                "Total level number, for example, there are 3 levels, including 0,1,2 levels.",
                null,
                false
            ))
            .add(integerProperty(
                NUM_SORTED_RUNS_COMPACTION_TRIGGER,
                "The sorted run number to trigger compaction. Includes level0 files (one file one sorted run) and " +
                    "high-level runs (one level one sorted run).",
                null,
                false
            ))
            .add(integerProperty(
                NUM_SORTED_RUNS_STOP_TRIGGER,
                "The number of sorted runs that trigger the stopping of writes, " +
                    "the default value is 'num-sorted-run.compaction-trigger' + 1.",
                null,
                false
            ))
            .add(stringProperty(
                ORC_BLOOM_FILTER_COLUMNS,
                "A comma-separated list of columns for which to create a bloon filter when writing.",
                null,
                false
            ))
            .add(doubleProperty(
                ORC_BLOOM_FILTER_FPP,
                "Define the default false positive probability for bloom filters.",
                null,
                false
            ))
            .add(stringProperty(
                PAGE_SIZE,
                "Memory page size.",
                null,
                false
            ))
            .add(booleanProperty(
                PARTIAL_UPDATE_IGNORE_DELETE,
                "Whether to ignore delete records in partial-update mode.",
                null,
                false
            ))
            .add(stringProperty(
                PARTITION_EXPIRATION_CHECK_INTERVAL,
                "The check interval of partition expiration.",
                null,
                false
            ))
            .add(stringProperty(
                PARTITION_EXPIRATION_TIME,
                "The expiration interval of a partition. A partition will be expired if" +
                    " it‘s lifetime is over this value. Partition time is extracted from" +
                    " the partition value.",
                null,
                false
            ))
            .add(stringProperty(
                PARTITION_TIMESTAMP_FORMATTER,
                "The formatter to format timestamp from string. It can be used" +
                    " with 'partition.timestamp-pattern' to create a formatter" +
                    " using the specified value.",
                null,
                false
            ))
            .add(stringProperty(
                PARTITION_TIMESTAMP_PATTERN,
                "You can specify a pattern to get a timestamp from partitions. " +
                    "The formatter pattern is defined by 'partition.timestamp-formatter'.",
                null,
                false
            ))
            .add(integerProperty(
                READ_BATCH_SIZE,
                "Read batch size for orc and parquet.",
                null,
                false
            ))
            .add(longProperty(
                SCAN_BOUNDED_WATERMARK,
                "End condition \"watermark\" for bounded streaming mode. Stream" +
                    " reading will end when a larger watermark snapshot is encountered.",
                null,
                false
            ))
            .add(integerProperty(
                SNAPSHOT_NUM_RETAINED_MIN,
                "The minimum number of completed snapshots to retain.",
                null,
                false
            ))
            .add(integerProperty(
                SNAPSHOT_NUM_RETAINED_MAX,
                "The maximum number of completed snapshots to retain.",
                null,
                false
            ))
            .add(stringProperty(
                SNAPSHOT_TIME_RETAINED,
                "The maximum time of completed snapshots to retain.",
                null,
                false
            ))
            .add(stringProperty(
                SOURCE_SPLIT_OPEN_FILE_COST,
                "Open file cost of a source file. It is used to avoid reading" +
                    " too many files with a source split, which can be very slow.",
                null,
                false
            ))
            .add(stringProperty(
                SOURCE_SPLIT_TARGET_SIZE,
                "Target size of a source split when scanning a bucket.",
                null,
                false
            ))
            .add(booleanProperty(
                STREAMING_READ_OVERWRITE,
                "Whether to read the changes from overwrite in streaming mode.",
                null,
                false
            ))
            .add(stringProperty(
                TARGET_FILE_SIZE,
                "Suggested file size of a manifest file.",
                null,
                false
            ))
            .add(stringProperty(
                WRITE_BUFFER_SIZE,
                "Amount of data to build up in memory before converting to a sorted on-disk file.",
                null,
                false
            ))
            .add(booleanProperty(
                WRITE_BUFFER_SPILLABLE,
                "Whether the write buffer can be spillable. Enabled by default when using object storage.",
                null,
                false
            ))
            .add(enumProperty(
                WRITE_MODE,
                "Specify the write mode for table.",
                WriteMode.class,
                null,
                false
            ))
            .add(booleanProperty(
                WRITE_ONLY,
                "If set to true, compactions and snapshot expiration will be skipped." +
                    "This option is used along with dedicated compact jobs.",
                null,
                false
            ))
            .build();
    }

    public List<PropertyMetadata<?>> getTableProperties() {
        return tableProperties;
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPrimaryKeys(Map<String, Object> tableProperties) {
        List<String> primaryKeys = (List<String>) tableProperties.get(PRIMARY_KEY_IDENTIFIER);
        return primaryKeys == null ? ImmutableList.of() : ImmutableList.copyOf(primaryKeys);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitionedKeys(Map<String, Object> tableProperties) {
        List<String> partitionedKeys = (List<String>) tableProperties.get(PARTITIONED_BY_PROPERTY);
        return partitionedKeys == null ? ImmutableList.of() : ImmutableList.copyOf(partitionedKeys);
    }
}
