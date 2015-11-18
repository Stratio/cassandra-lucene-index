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

import static com.stratio.cassandra.lucene.testsAT.varia.DataHelper.*;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@RunWith(JUnit4.class)
public class TokenRangeWithWideRowsMultiClusteringAT extends BaseAT {

    private static CassandraUtils cassandraUtils;

    @BeforeClass
    public static void before() {
        cassandraUtils = CassandraUtils.builder("token_wide_multi_clustering")
                                       .withPartitionKey("integer_1")
                                       .withClusteringKey("ascii_1", "double_1")
                                       .withColumn("ascii_1", "ascii")
                                       .withColumn("bigint_1", "bigint")
                                       .withColumn("blob_1", "blob")
                                       .withColumn("boolean_1", "boolean")
                                       .withColumn("decimal_1", "decimal")
                                       .withColumn("date_1", "timestamp")
                                       .withColumn("double_1", "double")
                                       .withColumn("float_1", "float")
                                       .withColumn("integer_1", "int")
                                       .withColumn("inet_1", "inet")
                                       .withColumn("text_1", "text")
                                       .withColumn("varchar_1", "varchar")
                                       .withColumn("uuid_1", "uuid")
                                       .withColumn("timeuuid_1", "timeuuid")
                                       .withColumn("list_1", "list<text>")
                                       .withColumn("set_1", "set<text>")
                                       .withColumn("map_1", "map<text,text>")
                                       .withColumn("lucene", "text")
                                       .build()
                                       .createKeyspace()
                                       .createTable()
                                       .createIndex()
                                       .insert(data1,
                                               data2,
                                               data3,
                                               data4,
                                               data5,
                                               data6,
                                               data7,
                                               data8,
                                               data9,
                                               data10,
                                               data11,
                                               data12,
                                               data13,
                                               data14,
                                               data15,
                                               data16,
                                               data17,
                                               data18,
                                               data19,
                                               data20)
                                       .refresh();
    }

    @AfterClass
    public static void after() {
        cassandraUtils.dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void tokenSearchTest1() {
        cassandraUtils.searchAll().and("AND TOKEN(integer_1) > TOKEN(1)").check(12);
    }

    @Test
    public void tokenSearchTest2() {
        cassandraUtils.searchAll().and("AND TOKEN(integer_1) >= TOKEN(1)").check(16);
    }

    @Test
    public void tokenSearchTest3() {
        cassandraUtils.searchAll().and("AND TOKEN(integer_1) < TOKEN(1)").check(4);
    }

    @Test
    public void tokenSearchTest4() {
        cassandraUtils.searchAll().and("AND TOKEN(integer_1) <= TOKEN(1)").check(8);
    }

    @Test
    public void tokenSearchTest5() {
        cassandraUtils.searchAll()
                      .and("AND TOKEN(integer_1) > TOKEN(1)")
                      .and("AND TOKEN(integer_1) < TOKEN(4)")
                      .check(4);
    }

    @Test
    public void tokenSearchTest6() {
        cassandraUtils.searchAll()
                      .and("AND TOKEN(integer_1) >= TOKEN(1)")
                      .and("AND TOKEN(integer_1) < TOKEN(4)")
                      .check(8);
    }

    @Test
    public void tokenSearchTest7() {
        cassandraUtils.searchAll()
                      .and("AND TOKEN(integer_1) > TOKEN(1)")
                      .and("AND TOKEN(integer_1) <= TOKEN(4)")
                      .check(8);
    }

    @Test
    public void tokenSearchTest8() {
        cassandraUtils.searchAll()
                      .and("AND TOKEN(integer_1) >= TOKEN(1)")
                      .and("AND TOKEN(integer_1) <= TOKEN(4)")
                      .check(12);
    }

    @Test
    public void tokenSearchTest9() {
        cassandraUtils.searchAll().and("AND TOKEN(integer_1) = TOKEN(1)").check(4);
    }

    @Test
    public void tokenSearchTest10() {
        cassandraUtils.searchAll().check(20);
    }

    @Test
    public void tokenClusteringSearchTest1() {
        cassandraUtils.searchAll().and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 > 1").check(1);
    }

    @Test
    public void tokenClusteringSearchTest2() {
        cassandraUtils.searchAll().and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 >= 1").check(2);
    }

    @Test
    public void tokenClusteringSearchTest3() {
        cassandraUtils.searchAll().and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 < 2").check(1);
    }

    @Test
    public void tokenClusteringSearchTest4() {
        cassandraUtils.searchAll().and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 <= 2").check(2);
    }

    @Test
    public void tokenClusteringSearchTest5() {
        cassandraUtils.searchAll().and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 = 1").check(1);
    }

    @Test
    public void tokenClusteringSearchTest6() {
        cassandraUtils.searchAll().and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 = 2").check(1);
    }

    @Test
    public void tokenWideClusteringSearchTest1() {
        cassandraUtils.searchAll().and("AND integer_1 = 2 AND ascii_1 > 'ascii'").check(2);
    }

    @Test
    public void tokenWideClusteringSearchTest2() {
        cassandraUtils.searchAll().and("AND integer_1 = 2 AND ascii_1 >= 'ascii'").check(4);
    }

    @Test
    public void tokenWideClusteringSearchTest3() {
        cassandraUtils.searchAll().and("AND integer_1 = 2 AND ascii_1 < 'ascii'").check(0);
    }

    @Test
    public void tokenWideClusteringSearchTest4() {
        cassandraUtils.searchAll().and("AND integer_1 = 2 AND ascii_1 <= 'ascii'").check(2);
    }
}
