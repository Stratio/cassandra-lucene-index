/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.cassandra.lucene.testsAT.varia;

import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.integerMapper;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@RunWith(JUnit4.class)
public class InOperatorWithSkinnyRowsAT extends BaseAT {

    private static final int NUM_PARTITIONS = 10;
    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("in_operator_with_skinny_rows")
                              .withPartitionKey("pk")
                              .withColumn("pk", "int", integerMapper())
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
}
