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

    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("token_wide_multi_clustering")
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
        utils.dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void testTokenSearch1() {
        utils.searchAll().and("AND TOKEN(integer_1) > TOKEN(1)").check(12);
    }

    @Test
    public void testTokenSearch2() {
        utils.searchAll().and("AND TOKEN(integer_1) >= TOKEN(1)").check(16);
    }

    @Test
    public void testTokenSearch3() {
        utils.searchAll().and("AND TOKEN(integer_1) < TOKEN(1)").check(4);
    }

    @Test
    public void testTokenSearch4() {
        utils.searchAll().and("AND TOKEN(integer_1) <= TOKEN(1)").check(8);
    }

    @Test
    public void testTokenSearch5() {
        utils.searchAll()
             .and("AND TOKEN(integer_1) > TOKEN(1)")
             .and("AND TOKEN(integer_1) < TOKEN(4)")
             .check(4);
    }

    @Test
    public void testTokenSearch6() {
        utils.searchAll()
             .and("AND TOKEN(integer_1) >= TOKEN(1)")
             .and("AND TOKEN(integer_1) < TOKEN(4)")
             .check(8);
    }

    @Test
    public void testTokenSearch7() {
        utils.searchAll()
             .and("AND TOKEN(integer_1) > TOKEN(1)")
             .and("AND TOKEN(integer_1) <= TOKEN(4)")
             .check(8);
    }

    @Test
    public void testTokenSearch8() {
        utils.searchAll()
             .and("AND TOKEN(integer_1) >= TOKEN(1)")
             .and("AND TOKEN(integer_1) <= TOKEN(4)")
             .check(12);
    }

    @Test
    public void testTokenSearch9() {
        utils.searchAll().and("AND TOKEN(integer_1) = TOKEN(1)").check(4);
    }

    @Test
    public void testTokenSearch10() {
        utils.searchAll().check(20);
    }

    @Test
    public void tokenClusteringSearchTest1() {
        utils.searchAll().and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 > 1").check(1);
    }

    @Test
    public void tokenClusteringSearchTest2() {
        utils.searchAll().and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 >= 1").check(2);
    }

    @Test
    public void tokenClusteringSearchTest3() {
        utils.searchAll().and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 < 2").check(1);
    }

    @Test
    public void tokenClusteringSearchTest4() {
        utils.searchAll().and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 <= 2").check(2);
    }

    @Test
    public void tokenClusteringSearchTest5() {
        utils.searchAll().and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 = 1").check(1);
    }

    @Test
    public void tokenClusteringSearchTest6() {
        utils.searchAll().and("AND integer_1 = 1 AND ascii_1 = 'ascii_bis' AND double_1 = 2").check(1);
    }

    @Test
    public void tokenWideClusteringSearchTest1() {
        utils.searchAll().and("AND integer_1 = 2 AND ascii_1 > 'ascii'").check(2);
    }

    @Test
    public void tokenWideClusteringSearchTest2() {
        utils.searchAll().and("AND integer_1 = 2 AND ascii_1 >= 'ascii'").check(4);
    }

    @Test
    public void tokenWideClusteringSearchTest3() {
        utils.searchAll().and("AND integer_1 = 2 AND ascii_1 < 'ascii'").check(0);
    }

    @Test
    public void tokenWideClusteringSearchTest4() {
        utils.searchAll().and("AND integer_1 = 2 AND ascii_1 <= 'ascii'").check(2);
    }
}
