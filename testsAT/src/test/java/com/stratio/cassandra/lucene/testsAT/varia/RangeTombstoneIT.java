/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.testsAT.varia;

import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;
import static org.junit.Assert.assertEquals;

/**
 * Tests the deletion of a slice of rows inside a partition with a RangeTombstone.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@RunWith(JUnit4.class)
public class RangeTombstoneIT extends BaseIT {

    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = builder("range_tombstone")
                .withTable("test")
                .withIndexName("idx")
                .withColumn("pk", "int")
                .withColumn("ck", "int")
                .withIndexColumn("lucene")
                .withPartitionKey("pk")
                .withClusteringKey("ck")
                .build()
                .createKeyspace()
                .createTable()
                .createIndex();
    }

    @AfterClass
    public static void after() {
        utils.dropKeyspace();
    }

    private void test(String clause, int expected) {
        String table = utils.getQualifiedTable();
        utils.truncateTable()
             .insert(new String[]{"pk", "ck"}, new Object[]{0, 0})
             .insert(new String[]{"pk", "ck"}, new Object[]{0, 1})
             .insert(new String[]{"pk", "ck"}, new Object[]{0, 2})
             .insert(new String[]{"pk", "ck"}, new Object[]{0, 3})
             .insert(new String[]{"pk", "ck"}, new Object[]{0, 4})
             .insert(new String[]{"pk", "ck"}, new Object[]{0, 5})
             .refresh()
             .execute(String.format("DELETE FROM %s WHERE pk = 0 AND %s", table, clause));
        utils.refresh()
             .searchAll().check(expected)
             .checkNumDocsInIndex(expected);
    }

    @Test
    public void testRangeTombstoneSingle() {
        test("ck = 1", 5);
    }

    @Test
    public void testRangeTombstoneCloseClose() {
        test("ck >= 1 AND ck <= 4", 2);
    }

    @Test
    public void testRangeTombstoneOpenOpen() {
        test("ck > 1 AND ck < 4", 4);
    }

    @Test
    public void testRangeTombstoneOpenClose() {
        test("ck > 1 AND ck <= 4", 3);
    }

    @Test
    public void testRangeTombstoneCloseOpen() {
        test("ck >= 1 AND ck < 4", 3);
    }

    @Test
    public void testRangeTombstoneNone() {
        test("ck > 6", 6);
    }

    @Test
    public void testRangeTombstoneAll() {
        test("ck > -1 AND ck < 10", 0);
    }

    @Test
    public void testRangeTombstoneOpenInfinite() {
        test("ck > 1", 2);
    }

    @Test
    public void testRangeTombstoneCloseInfinite() {
        test("ck >= 1", 1);
    }

    @Test
    public void testRangeTombstoneInfiniteOpen() {
        test("ck < 4", 2);
    }

    @Test
    public void testRangeTombstoneInfiniteClose() {
        test("ck <= 4", 1);
    }
}