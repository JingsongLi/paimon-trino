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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

/** A simple table test helper to write and commit. */
public class SimpleTableTestHelper {

    private final TableWrite writer;
    private final TableCommit commit;

    public SimpleTableTestHelper(Path path, RowType rowType) throws Exception {
        new SchemaManager(path)
                .commitNewVersion(
                        new UpdateSchema(
                                rowType,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                new HashMap<>(),
                                ""));
        FileStoreTable table = FileStoreTableFactory.create(path);
        this.writer = table.newWrite();
        this.commit = table.newCommit("user");
    }

    public void write(RowData row) throws Exception {
        writer.write(row);
    }

    public void commit() throws Exception {
        commit.commit(UUID.randomUUID().toString(), writer.prepareCommit(true));
    }
}
