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

package org.apache.flink.table.store.trino;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** Trino {@link ConnectorTableHandle}. */
public final class TrinoTableHandle implements ConnectorTableHandle {
    private final String schemaName;
    private final String tableName;
    private final String location;
    private final long tableSchemaId;
    private final TupleDomain<TrinoColumnHandle> filter;
    private final Optional<List<ColumnHandle>> projectedColumns;

    private TableSchema tableSchema;

    public TrinoTableHandle(
            String schemaName, String tableName, String location, long tableSchemaId) {
        this(schemaName, tableName, location, tableSchemaId, TupleDomain.all(), Optional.empty());
    }

    @JsonCreator
    public TrinoTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("location") String location,
            @JsonProperty("tableSchemaId") long tableSchemaId,
            @JsonProperty("filter") TupleDomain<TrinoColumnHandle> filter,
            @JsonProperty("projection") Optional<List<ColumnHandle>> projectedColumns) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.location = location;
        this.tableSchemaId = tableSchemaId;
        this.filter = filter;
        this.projectedColumns = projectedColumns;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public String getLocation() {
        return location;
    }

    @JsonProperty
    public long getTableSchemaId() {
        return tableSchemaId;
    }

    @JsonProperty
    public TupleDomain<TrinoColumnHandle> getFilter() {
        return filter;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getProjectedColumns() {
        return projectedColumns;
    }

    public TrinoTableHandle copy(TupleDomain<TrinoColumnHandle> filter) {
        return new TrinoTableHandle(
                schemaName, tableName, location, tableSchemaId, filter, projectedColumns);
    }

    public TrinoTableHandle copy(Optional<List<ColumnHandle>> projectedColumns) {
        return new TrinoTableHandle(
                schemaName, tableName, location, tableSchemaId, filter, projectedColumns);
    }

    public TableSchema tableSchema() {
        if (tableSchema == null) {
            tableSchema = new SchemaManager(new Path(location)).schema(tableSchemaId);
        }
        return tableSchema;
    }

    public ConnectorTableMetadata tableMetadata() {
        return new ConnectorTableMetadata(
                SchemaTableName.schemaTableName(schemaName, tableName),
                columnMetadatas(),
                Collections.emptyMap(),
                Optional.ofNullable(tableSchema().comment()));
    }

    public List<ColumnMetadata> columnMetadatas() {
        return tableSchema().fields().stream()
                .map(
                        column ->
                                ColumnMetadata.builder()
                                        .setName(column.name())
                                        .setType(
                                                TrinoTypeUtils.fromFlinkType(
                                                        column.type().logicalType()))
                                        .setNullable(column.type().logicalType().isNullable())
                                        .setComment(Optional.ofNullable(column.description()))
                                        .build())
                .collect(Collectors.toList());
    }

    public TrinoColumnHandle columnHandle(String field) {
        int index = tableSchema().fieldNames().indexOf(field);
        return TrinoColumnHandle.of(field, tableSchema().fields().get(index).type().logicalType());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TrinoTableHandle that = (TrinoTableHandle) o;
        return tableSchemaId == that.tableSchemaId
                && Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(location, that.location)
                && Objects.equals(filter, that.filter)
                && Objects.equals(projectedColumns, that.projectedColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schemaName, tableName, location, tableSchemaId, filter, projectedColumns);
    }
}
