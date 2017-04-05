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

import static com.stratio.cassandra.lucene.builder.Builder.*;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@RunWith(JUnit4.class)
public class InOperatorWithWideRowsIT extends BaseIT {

    private static final int NUM_PARTITIONS = 10;
    private static final int PARTITION_SIZE = 10;
    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("in_operator_with_wide_rows")
                              .withIndexColumn(null)
                              .withUseNewQuerySyntax(true)
                              .withPartitionKey("pk")
                              .withClusteringKey("ck")
                              .withColumn("pk", "int", integerMapper())
                              .withColumn("ck", "int", integerMapper())
                              .withColumn("rc", "int", integerMapper())
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            for (int j = 0; j < PARTITION_SIZE; j++) {
                utils.insert(new String[]{"pk", "ck", "rc"}, new Object[]{i, j, i * NUM_PARTITIONS + j});
            }
        }
        utils.refresh();
    }

    @AfterClass
    public static void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    @Test
    public void testSearchAllWithPartitionKeyIn() {
        utils.searchAll()
             .and("AND pk IN (0, 9)")
             .checkOrderedColumns("rc", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99);
    }

    @Test
    public void testSearchAllWithReversedPartitionKeyIn() {
        utils.searchAll()
             .and("AND pk IN (9, 0)")
             .checkOrderedColumns("rc", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99);
    }

    @Test
    public void testSearchAllWithWithBothKeysIn() {
        utils.searchAll().and("AND pk IN (0, 9) AND ck IN (0, 9)").checkOrderedColumns("rc", 0, 9, 90, 99);
    }

    @Test
    public void testSearchAllReversedWithBothKeysIn() {
        utils.searchAll().and("AND pk IN (9, 0) AND ck IN (9, 0)").checkOrderedColumns("rc", 0, 9, 90, 99);
    }

    @Test
    public void testQueryWithPartitionKeyIn() {
        utils.query(all())
             .and("AND pk IN (9, 0)")
             .checkOrderedColumns("rc", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99);
    }

    @Test
    public void testQueryWithBothKeysIn() {
        utils.query(all()).and("AND pk IN (9, 0) AND ck IN (9, 0)").checkOrderedColumns("rc", 0, 9, 90, 99);
    }

    @Test
    public void testSortWithPartitionKeyIn() {
        utils.sort(field("pk"))
             .and("AND pk IN (9, 0)")
             .checkOrderedColumns("rc", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99);
    }

    @Test
    public void testSortWithBothKeysIn() {
        utils.sort(field("pk")).and("AND pk IN (9, 0) AND ck IN (9, 0)").checkOrderedColumns("rc", 0, 9, 90, 99);
    }
}
