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
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableScan;

import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.List;
import java.util.stream.Collectors;

/** Trino {@link ConnectorSplitManager}. */
public abstract class TrinoSplitManagerBase implements ConnectorSplitManager {

    protected ConnectorSplitSource getSplits(ConnectorTableHandle connectorTableHandle) {
        // TODO dynamicFilter?
        // TODO what is constraint?

        TrinoTableHandle tableHandle = (TrinoTableHandle) connectorTableHandle;
        Table table = tableHandle.table();
        TableScan tableScan = table.newScan();
        new TrinoFilterConverter(table.rowType())
                .convert(tableHandle.getFilter())
                .ifPresent(tableScan::withFilter);
        List<Split> splits = tableScan.plan().splits();
        return new TrinoSplitSource(
                splits.stream().map(TrinoSplit::fromSplit).collect(Collectors.toList()));
    }
}
