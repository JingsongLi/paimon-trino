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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.utils.StringUtils;

import java.util.Map;

import static org.apache.paimon.trino.TrinoTableOptions.BUCKET;
import static org.apache.paimon.trino.TrinoTableOptions.BUCKET_KEY;
import static org.apache.paimon.trino.TrinoTableOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.trino.TrinoTableOptions.COMMIT_FORCE_COMPACT;
import static org.apache.paimon.trino.TrinoTableOptions.COMPACTION_MAX_FILE_NUM;
import static org.apache.paimon.trino.TrinoTableOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT;
import static org.apache.paimon.trino.TrinoTableOptions.COMPACTION_MIN_FILE_NUM;
import static org.apache.paimon.trino.TrinoTableOptions.COMPACTION_SIZE_RATIO;
import static org.apache.paimon.trino.TrinoTableOptions.CONTINUOUS_DISCOVERY_INTERVAL;
import static org.apache.paimon.trino.TrinoTableOptions.DYNAMIC_PARTITION_OVERWRITE;
import static org.apache.paimon.trino.TrinoTableOptions.FILE_FORMAT_PROPERTY;
import static org.apache.paimon.trino.TrinoTableOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.trino.TrinoTableOptions.LOG_CHANGELOG_MODE;
import static org.apache.paimon.trino.TrinoTableOptions.LOG_CONSISTENCY;
import static org.apache.paimon.trino.TrinoTableOptions.LOG_FORMAT;
import static org.apache.paimon.trino.TrinoTableOptions.LOG_SCAN_REMOVE_NORMALIZE;
import static org.apache.paimon.trino.TrinoTableOptions.LOOKUP_CACHE_FILE_RETENTION;
import static org.apache.paimon.trino.TrinoTableOptions.LOOKUP_CACHE_MAX_DISK_SIZE;
import static org.apache.paimon.trino.TrinoTableOptions.LOOKUP_CACHE_MAX_MEMORY_SIZE;
import static org.apache.paimon.trino.TrinoTableOptions.LOOKUP_HASH_LOAD_FACTOR;
import static org.apache.paimon.trino.TrinoTableOptions.MANIFEST_FORMAT;
import static org.apache.paimon.trino.TrinoTableOptions.MANIFEST_MERGE_MIN_COUNT;
import static org.apache.paimon.trino.TrinoTableOptions.MANIFEST_TARGET_FILE_SIZE;
import static org.apache.paimon.trino.TrinoTableOptions.MERGE_ENGINE;
import static org.apache.paimon.trino.TrinoTableOptions.NUM_LEVELS;
import static org.apache.paimon.trino.TrinoTableOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER;
import static org.apache.paimon.trino.TrinoTableOptions.NUM_SORTED_RUNS_STOP_TRIGGER;
import static org.apache.paimon.trino.TrinoTableOptions.ORC_BLOOM_FILTER_COLUMNS;
import static org.apache.paimon.trino.TrinoTableOptions.ORC_BLOOM_FILTER_FPP;
import static org.apache.paimon.trino.TrinoTableOptions.PAGE_SIZE;
import static org.apache.paimon.trino.TrinoTableOptions.PARTIAL_UPDATE_IGNORE_DELETE;
import static org.apache.paimon.trino.TrinoTableOptions.PARTITION_EXPIRATION_CHECK_INTERVAL;
import static org.apache.paimon.trino.TrinoTableOptions.PARTITION_EXPIRATION_TIME;
import static org.apache.paimon.trino.TrinoTableOptions.PARTITION_TIMESTAMP_FORMATTER;
import static org.apache.paimon.trino.TrinoTableOptions.PARTITION_TIMESTAMP_PATTERN;
import static org.apache.paimon.trino.TrinoTableOptions.READ_BATCH_SIZE;
import static org.apache.paimon.trino.TrinoTableOptions.SCAN_BOUNDED_WATERMARK;
import static org.apache.paimon.trino.TrinoTableOptions.SNAPSHOT_NUM_RETAINED_MAX;
import static org.apache.paimon.trino.TrinoTableOptions.SNAPSHOT_NUM_RETAINED_MIN;
import static org.apache.paimon.trino.TrinoTableOptions.SNAPSHOT_TIME_RETAINED;
import static org.apache.paimon.trino.TrinoTableOptions.SOURCE_SPLIT_OPEN_FILE_COST;
import static org.apache.paimon.trino.TrinoTableOptions.SOURCE_SPLIT_TARGET_SIZE;
import static org.apache.paimon.trino.TrinoTableOptions.STREAMING_READ_OVERWRITE;
import static org.apache.paimon.trino.TrinoTableOptions.TARGET_FILE_SIZE;
import static org.apache.paimon.trino.TrinoTableOptions.WRITE_BUFFER_SIZE;
import static org.apache.paimon.trino.TrinoTableOptions.WRITE_BUFFER_SPILLABLE;
import static org.apache.paimon.trino.TrinoTableOptions.WRITE_MODE;
import static org.apache.paimon.trino.TrinoTableOptions.WRITE_ONLY;

/** Trino table option util. */
public class TrinoTableOptionUtils {

    public static void buildOptions(Schema.Builder builder, Map<String, Object> properties) {
        if (properties.get(FILE_FORMAT_PROPERTY) != null) {
            builder.option(CoreOptions.FILE_FORMAT.key(), String.valueOf(properties.get(FILE_FORMAT_PROPERTY)));
        }

        if (properties.get(CHANGELOG_PRODUCER) != null) {
            builder.option(CoreOptions.CHANGELOG_PRODUCER.key(), String.valueOf(properties.get(CHANGELOG_PRODUCER)));
        }

        if (properties.get(COMMIT_FORCE_COMPACT) != null) {
            builder.option(CoreOptions.COMMIT_FORCE_COMPACT.key(), String.valueOf(properties.get(COMMIT_FORCE_COMPACT)));
        }

        if (properties.get(BUCKET) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(BUCKET)))) {
            builder.option(CoreOptions.BUCKET.key(), String.valueOf(properties.get(BUCKET)));
        }

        if (properties.get(BUCKET_KEY) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(BUCKET_KEY)))) {
            builder.option(CoreOptions.BUCKET_KEY.key(), String.valueOf(properties.get(BUCKET_KEY)));
        }

        if (properties.get(MERGE_ENGINE) != null) {
            builder.option(CoreOptions.MERGE_ENGINE.key(), String.valueOf(properties.get(MERGE_ENGINE)));
        }

        if (properties.get(COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT) != null) {
            builder.option(CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT.key(),
                String.valueOf(properties.get(COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT)));
        }

        if (properties.get(COMPACTION_MAX_FILE_NUM) != null) {
            builder.option(CoreOptions.COMPACTION_MAX_FILE_NUM.key(),
                String.valueOf(properties.get(COMPACTION_MAX_FILE_NUM)));
        }

        if (properties.get(COMPACTION_MIN_FILE_NUM) != null) {
            builder.option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(),
                String.valueOf(properties.get(COMPACTION_MIN_FILE_NUM)));
        }

        if (properties.get(COMPACTION_SIZE_RATIO) != null) {
            builder.option(CoreOptions.COMPACTION_SIZE_RATIO.key(),
                String.valueOf(properties.get(COMPACTION_SIZE_RATIO)));
        }

        if (properties.get(CONTINUOUS_DISCOVERY_INTERVAL) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(CONTINUOUS_DISCOVERY_INTERVAL)))) {
            builder.option(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL.key(),
                String.valueOf(properties.get(CONTINUOUS_DISCOVERY_INTERVAL)));
        }

        if (properties.get(DYNAMIC_PARTITION_OVERWRITE) != null) {
            builder.option(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(),
                String.valueOf(properties.get(DYNAMIC_PARTITION_OVERWRITE)));
        }

        if (properties.get(FULL_COMPACTION_DELTA_COMMITS) != null) {
            builder.option(CoreOptions.FULL_COMPACTION_DELTA_COMMITS.key(),
                String.valueOf(properties.get(FULL_COMPACTION_DELTA_COMMITS)));
        }

        if (properties.get(LOG_CHANGELOG_MODE) != null) {
            builder.option(CoreOptions.LOG_CHANGELOG_MODE.key(),
                String.valueOf(properties.get(LOG_CHANGELOG_MODE)));
        }

        if (properties.get(LOG_CONSISTENCY) != null) {
            builder.option(CoreOptions.LOG_CONSISTENCY.key(), String.valueOf(properties.get(LOG_CONSISTENCY)));
        }

        if (properties.get(LOG_FORMAT) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(LOG_FORMAT)))) {
            builder.option(CoreOptions.LOG_FORMAT.key(), String.valueOf(properties.get(LOG_FORMAT)));
        }

        if (properties.get(LOG_SCAN_REMOVE_NORMALIZE) != null) {
            builder.option(CoreOptions.LOG_SCAN_REMOVE_NORMALIZE.key(),
                String.valueOf(properties.get(LOG_SCAN_REMOVE_NORMALIZE)));
        }

        if (properties.get(LOOKUP_CACHE_FILE_RETENTION) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(LOOKUP_CACHE_FILE_RETENTION)))) {
            builder.option(CoreOptions.LOG_FORMAT.key(), String.valueOf(properties.get(LOOKUP_CACHE_FILE_RETENTION)));
        }

        if (properties.get(LOOKUP_CACHE_MAX_DISK_SIZE) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(LOOKUP_CACHE_MAX_DISK_SIZE)))) {
            builder.option(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE.key(),
                String.valueOf(properties.get(LOOKUP_CACHE_MAX_DISK_SIZE)));
        }

        if (properties.get(LOOKUP_CACHE_MAX_MEMORY_SIZE) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(LOOKUP_CACHE_MAX_MEMORY_SIZE)))) {
            builder.option(CoreOptions.LOOKUP_CACHE_MAX_MEMORY_SIZE.key(),
                String.valueOf(properties.get(LOOKUP_CACHE_MAX_MEMORY_SIZE)));
        }

        if (properties.get(LOOKUP_HASH_LOAD_FACTOR) != null) {
            builder.option(CoreOptions.LOOKUP_HASH_LOAD_FACTOR.key(),
                String.valueOf(properties.get(LOOKUP_HASH_LOAD_FACTOR)));
        }

        if (properties.get(MANIFEST_FORMAT) != null) {
            builder.option(CoreOptions.MANIFEST_FORMAT.key(), String.valueOf(properties.get(MANIFEST_FORMAT)));
        }

        if (properties.get(MANIFEST_MERGE_MIN_COUNT) != null) {
            builder.option(CoreOptions.MANIFEST_MERGE_MIN_COUNT.key(),
                String.valueOf(properties.get(MANIFEST_MERGE_MIN_COUNT)));
        }

        if (properties.get(MANIFEST_TARGET_FILE_SIZE) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(MANIFEST_TARGET_FILE_SIZE)))) {
            builder.option(CoreOptions.MANIFEST_TARGET_FILE_SIZE.key(),
                String.valueOf(properties.get(MANIFEST_TARGET_FILE_SIZE)));
        }

        if (properties.get(NUM_LEVELS) != null) {
            builder.option(CoreOptions.NUM_LEVELS.key(), String.valueOf(properties.get(NUM_LEVELS)));
        }

        if (properties.get(NUM_SORTED_RUNS_COMPACTION_TRIGGER) != null) {
            builder.option(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(),
                String.valueOf(properties.get(NUM_SORTED_RUNS_COMPACTION_TRIGGER)));
        }

        if (properties.get(NUM_SORTED_RUNS_STOP_TRIGGER) != null) {
            builder.option(CoreOptions.NUM_SORTED_RUNS_STOP_TRIGGER.key(),
                String.valueOf(properties.get(NUM_SORTED_RUNS_STOP_TRIGGER)));
        }

        if (properties.get(ORC_BLOOM_FILTER_COLUMNS) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(ORC_BLOOM_FILTER_COLUMNS)))) {
            builder.option(CoreOptions.ORC_BLOOM_FILTER_COLUMNS.key(),
                String.valueOf(properties.get(ORC_BLOOM_FILTER_COLUMNS)));
        }

        if (properties.get(ORC_BLOOM_FILTER_FPP) != null) {
            builder.option(CoreOptions.ORC_BLOOM_FILTER_FPP.key(),
                String.valueOf(properties.get(ORC_BLOOM_FILTER_FPP)));
        }

        if (properties.get(PAGE_SIZE) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(PAGE_SIZE)))) {
            builder.option(CoreOptions.PAGE_SIZE.key(), String.valueOf(properties.get(PAGE_SIZE)));
        }

        if (properties.get(PARTIAL_UPDATE_IGNORE_DELETE) != null) {
            builder.option(CoreOptions.PARTIAL_UPDATE_IGNORE_DELETE.key(),
                String.valueOf(properties.get(PARTIAL_UPDATE_IGNORE_DELETE)));
        }

        if (properties.get(PARTITION_EXPIRATION_CHECK_INTERVAL) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(
                properties.get(PARTITION_EXPIRATION_CHECK_INTERVAL)))) {
            builder.option(CoreOptions.PARTITION_EXPIRATION_CHECK_INTERVAL.key(),
                String.valueOf(properties.get(PARTITION_EXPIRATION_CHECK_INTERVAL)));
        }

        if (properties.get(PARTITION_EXPIRATION_TIME) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(PARTITION_EXPIRATION_TIME)))) {
            builder.option(CoreOptions.PARTITION_EXPIRATION_TIME.key(),
                String.valueOf(properties.get(PARTITION_EXPIRATION_TIME)));
        }

        if (properties.get(PARTITION_TIMESTAMP_FORMATTER) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(PARTITION_TIMESTAMP_FORMATTER)))) {
            builder.option(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(),
                String.valueOf(properties.get(PARTITION_TIMESTAMP_FORMATTER)));
        }

        if (properties.get(PARTITION_TIMESTAMP_PATTERN) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(PARTITION_TIMESTAMP_PATTERN)))) {
            builder.option(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(),
                String.valueOf(properties.get(PARTITION_TIMESTAMP_PATTERN)));
        }

        if (properties.get(READ_BATCH_SIZE) != null) {
            builder.option(CoreOptions.READ_BATCH_SIZE.key(), String.valueOf(properties.get(READ_BATCH_SIZE)));
        }


        if (properties.get(SCAN_BOUNDED_WATERMARK) != null) {
            builder.option(CoreOptions.SCAN_BOUNDED_WATERMARK.key(),
                String.valueOf(properties.get(SCAN_BOUNDED_WATERMARK)));
        }

        if (properties.get(SNAPSHOT_NUM_RETAINED_MIN) != null) {
            builder.option(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(),
                String.valueOf(properties.get(SNAPSHOT_NUM_RETAINED_MIN)));
        }

        if (properties.get(SNAPSHOT_NUM_RETAINED_MAX) != null) {
            builder.option(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(),
                String.valueOf(properties.get(SNAPSHOT_NUM_RETAINED_MAX)));
        }

        if (properties.get(SNAPSHOT_TIME_RETAINED) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(SNAPSHOT_TIME_RETAINED)))) {
            builder.option(CoreOptions.SNAPSHOT_TIME_RETAINED.key(),
                String.valueOf(properties.get(SNAPSHOT_TIME_RETAINED)));
        }

        if (properties.get(SOURCE_SPLIT_OPEN_FILE_COST) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(SOURCE_SPLIT_OPEN_FILE_COST)))) {
            builder.option(CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(),
                String.valueOf(properties.get(SOURCE_SPLIT_OPEN_FILE_COST)));
        }

        if (properties.get(SOURCE_SPLIT_TARGET_SIZE) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(SOURCE_SPLIT_TARGET_SIZE)))) {
            builder.option(CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(),
                String.valueOf(properties.get(SOURCE_SPLIT_TARGET_SIZE)));
        }

        if (properties.get(STREAMING_READ_OVERWRITE) != null) {
            builder.option(CoreOptions.STREAMING_READ_OVERWRITE.key(),
                String.valueOf(properties.get(STREAMING_READ_OVERWRITE)));
        }

        if (properties.get(TARGET_FILE_SIZE) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(TARGET_FILE_SIZE)))) {
            builder.option(CoreOptions.TARGET_FILE_SIZE.key(),
                String.valueOf(properties.get(TARGET_FILE_SIZE)));
        }

        if (properties.get(WRITE_BUFFER_SIZE) != null
            && !StringUtils.isNullOrWhitespaceOnly(String.valueOf(properties.get(WRITE_BUFFER_SIZE)))) {
            builder.option(CoreOptions.WRITE_BUFFER_SIZE.key(),
                String.valueOf(properties.get(WRITE_BUFFER_SIZE)));
        }

        if (properties.get(WRITE_BUFFER_SPILLABLE) != null) {
            builder.option(CoreOptions.WRITE_BUFFER_SPILLABLE.key(),
                String.valueOf(properties.get(WRITE_BUFFER_SPILLABLE)));
        }

        if (properties.get(WRITE_MODE) != null) {
            builder.option(CoreOptions.WRITE_MODE.key(), String.valueOf(properties.get(WRITE_MODE)));
        }

        if (properties.get(WRITE_ONLY) != null) {
            builder.option(CoreOptions.WRITE_ONLY.key(), String.valueOf(properties.get(WRITE_ONLY)));
        }
    }
}
