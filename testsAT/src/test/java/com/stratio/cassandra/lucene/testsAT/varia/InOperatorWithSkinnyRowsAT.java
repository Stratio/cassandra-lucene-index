/**
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

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.stratio.cassandra.lucene.testsAT.BaseAT;
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
public class InOperatorWithSkinnyRowsAT extends BaseAT {

    private static String TOPK_ERROR = "Top-k searches can't be directed to more than one partition, " +
                                       "you should either direct them to one single partition or " +
                                       "don't use any partition restrictions.";

    private static final int NUM_PARTITIONS = 10;
    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("in_operator_with_skinny_rows")
                              .withPartitionKey("pk")
                              .withColumn("pk", "int", integerMapper().sorted(true))
                              .withColumn("rc", "int", integerMapper())
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            utils.insert(new String[]{"pk", "rc"}, new Object[]{i, i});
        }
        utils.waitForIndexing().refresh();
    }

    @AfterClass
    public static void after() {
        utils.dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void partitionKeyInTest() {
        utils.searchAll().fetchSize(4).and("AND pk IN (0, 5, 9)").checkIntColumn("rc", 0, 5, 9);
    }

    @Test
    public void reversedPartitionKeyInTest() {
        utils.searchAll().fetchSize(4).and("AND pk IN (9, 5, 0)").checkIntColumn("rc", 0, 5, 9);
    }

    @Test
    public void queryWithInTest() {
        utils.query(all()).and("AND pk IN (9, 5, 0)").check(InvalidQueryException.class, TOPK_ERROR);
    }

    @Test
    public void sortWithInTest() {
        utils.sort(field("pk")).and("AND pk IN (9, 5, 0)").check(InvalidQueryException.class, TOPK_ERROR);
    }
}
