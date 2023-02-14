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

import org.apache.flink.table.store.table.Table;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.types.logical.RowType;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.trino.ClassLoaderUtils.runWithContextClassLoader;

/** Trino {@link ConnectorPageSourceProvider}. */
public class TrinoPageSourceProvider implements ConnectorPageSourceProvider {

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {
        return runWithContextClassLoader(
                () -> createPageSource((TrinoTableHandle) table, (TrinoSplit) split, columns),
                TrinoPageSourceProvider.class.getClassLoader());
    }

    private ConnectorPageSource createPageSource(TrinoTableHandle tableHandle, TrinoSplit split, List<ColumnHandle> columns) {
        Table table = tableHandle.table();
        TableRead read = table.newRead();
        RowType rowType = table.rowType();
        List<String> fieldNames = rowType.getFieldNames();
        List<String> projectedFields =
                columns.stream()
                        .map(TrinoColumnHandle.class::cast)
                        .map(TrinoColumnHandle::getColumnName)
                        .collect(Collectors.toList());
        if (!fieldNames.equals(projectedFields)) {
            int[] projected = projectedFields.stream().mapToInt(fieldNames::indexOf).toArray();
            read.withProjection(projected);
        }

        new TrinoFilterConverter(rowType)
                .convert(tableHandle.getFilter())
                .ifPresent(read::withFilter);

        try {
            return new TrinoPageSource(read.createReader(split.decodeSplit()), columns);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
