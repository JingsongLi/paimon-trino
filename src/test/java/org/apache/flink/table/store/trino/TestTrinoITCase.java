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
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.apache.flink.table.data.StringData.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for trino connector. */
public class TestTrinoITCase extends AbstractTestQueryFramework {

    private static final String CATALOG = "tablestore";
    private static final String DB = "default";

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        String warehouse =
                Files.createTempDirectory(UUID.randomUUID().toString()).toUri().toString();
        // flink sink
        Path tablePath1 = new Path(warehouse, DB + ".db/t1");
        SimpleTableTestHelper testHelper1 = createTestHelper(tablePath1);
        testHelper1.write(GenericRowData.of(1, 2L, fromString("1"), fromString("1")));
        testHelper1.write(GenericRowData.of(3, 4L, fromString("2"), fromString("2")));
        testHelper1.write(GenericRowData.of(5, 6L, fromString("3"), fromString("3")));
        testHelper1.write(GenericRowData.ofKind(RowKind.DELETE, 3, 4L, fromString("2"), fromString("2")));
        testHelper1.commit();

        Path tablePath2 = new Path(warehouse, "default.db/t2");
        SimpleTableTestHelper testHelper2 = createTestHelper(tablePath2);
        testHelper2.write(GenericRowData.of(1, 2L, fromString("1"), fromString("1")));
        testHelper2.write(GenericRowData.of(3, 4L, fromString("2"), fromString("2")));
        testHelper2.commit();
        testHelper2.write(GenericRowData.of(5, 6L, fromString("3"), fromString("3")));
        testHelper2.write(GenericRowData.of(7, 8L, fromString("4"), fromString("4")));
        testHelper2.commit();

        {
            Path tablePath3 = new Path(warehouse, "default.db/t3");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new RowType.RowField("pt", new VarCharType()),
                                    new RowType.RowField("a", new IntType()),
                                    new RowType.RowField("b", new BigIntType()),
                                    new RowType.RowField("c", new BigIntType()),
                                    new RowType.RowField("d", new IntType())));
            new SchemaManager(tablePath3)
                    .commitNewVersion(
                            new UpdateSchema(
                                    rowType,
                                    Collections.singletonList("pt"),
                                    Collections.emptyList(),
                                    new HashMap<>(),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(tablePath3);
            TableWrite writer = table.newWrite("user");
            TableCommit commit = table.newCommit("user");
            writer.write(GenericRowData.of(fromString("1"), 1, 1L, 1L, 1));
            writer.write(GenericRowData.of(fromString("1"), 1, 2L, 2L, 2));
            writer.write(GenericRowData.of(fromString("2"), 3, 3L, 3L, 3));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        {
            Path tablePath4 = new Path(warehouse, "default.db/t4");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new RowType.RowField("i", new IntType()),
                                    new RowType.RowField("map", new MapType(new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH)))
                                    ));
            new SchemaManager(tablePath4)
                    .commitNewVersion(
                            new UpdateSchema(
                                    rowType,
                                    Collections.emptyList(),
                                    Collections.singletonList("i"),
                                    new HashMap<>(),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(tablePath4);
            TableWrite writer = table.newWrite("user");
            TableCommit commit = table.newCommit("user");
            writer.write(GenericRowData.of(1, new GenericMapData(new HashMap<>() {
                {
                    put(fromString("1"), fromString("2"));
                }
            })));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner =
                    DistributedQueryRunner.builder(
                                    testSessionBuilder().setCatalog(CATALOG).setSchema(DB).build())
                            .build();
            queryRunner.installPlugin(new TrinoPlugin());
            Map<String, String> options = new HashMap<>();
            options.put("warehouse", warehouse);
            queryRunner.createCatalog(CATALOG, CATALOG, options);
            return queryRunner;
        } catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static SimpleTableTestHelper createTestHelper(Path tablePath) throws Exception {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("a", new IntType()),
                                new RowType.RowField("b", new BigIntType()),
                                // test field name has upper case
                                new RowType.RowField("aCa", new VarCharType()),
                                new RowType.RowField("d", new CharType(1))
                                ));
        return new SimpleTableTestHelper(tablePath, rowType);
    }

    @Test
    public void testComplexTypes() {
        assertThat(sql("SELECT * FROM tablestore.default.t4")).isEqualTo("[[1, {1=2}]]");
    }

    @Test
    public void testProjection() {
        assertThat(sql("SELECT * FROM tablestore.default.t1")).isEqualTo("[[1, 2, 1, 1], [5, 6, 3, 3]]");
        assertThat(sql("SELECT a, aCa FROM tablestore.default.t1")).isEqualTo("[[1, 1], [5, 3]]");
        assertThat(sql("SELECT SUM(b) FROM tablestore.default.t1")).isEqualTo("[[8]]");
    }

    @Test
    public void testSystemTable() {
        assertThat(sql("SELECT snapshot_id,schema_id,commit_user,commit_identifier,commit_kind FROM \"t1$snapshots\"")).isEqualTo("[[1, 0, user, 0, APPEND]]");
    }

    @Test
    public void testFilter() {
        assertThat(sql("SELECT a, aCa FROM tablestore.default.t2 WHERE a < 4"))
                .isEqualTo("[[1, 1], [3, 2]]");
    }

    @Test
    public void testGroupByWithCast() {
        assertThat(sql("SELECT pt, a, SUM(b), SUM(d) FROM tablestore.default.t3 GROUP BY pt, a ORDER BY pt, a"))
                .isEqualTo("[[1, 1, 3, 3], [2, 3, 3, 3]]");
    }

    private String sql(String sql) {
        MaterializedResult result = getQueryRunner().execute(sql);
        return result.getMaterializedRows().toString();
    }
}
