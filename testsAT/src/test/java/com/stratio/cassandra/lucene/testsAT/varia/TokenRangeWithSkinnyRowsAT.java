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

@RunWith(JUnit4.class)
public class TokenRangeWithSkinnyRowsAT extends BaseAT {

    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("token_range_skinny_rows")
                              .withPartitionKey("integer_1", "ascii_1")
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
    public void tokenSearchTest1() {
        utils.searchAll().and("AND TOKEN(integer_1, ascii_1) > TOKEN(1, 'ascii')").check(4);
    }

    @Test
    public void tokenSearchTest2() {
        utils.searchAll().and("AND TOKEN(integer_1, ascii_1) >= TOKEN(1, 'ascii')").check(5);
    }

    @Test
    public void tokenSearchTest3() {
        utils.searchAll().and("AND TOKEN(integer_1, ascii_1) < TOKEN(1, 'ascii')").check(5);
    }

    @Test
    public void tokenSearchTest4() {
        utils.searchAll().and("AND TOKEN(integer_1, ascii_1) <= TOKEN(1, 'ascii')").check(6);
    }

    @Test
    public void tokenSearchTest5() {
        utils.searchAll()
             .and("AND TOKEN(integer_1, ascii_1) > TOKEN(1, 'ascii')")
             .and("AND TOKEN(integer_1, ascii_1) < TOKEN(3, 'ascii')")
             .check(3);
    }

    @Test
    public void tokenSearchTest6() {
        utils.searchAll()
             .and("AND TOKEN(integer_1, ascii_1) >= TOKEN(1, 'ascii')")
             .and("AND TOKEN(integer_1, ascii_1) < TOKEN(3, 'ascii')")
             .check(4);
    }

    @Test
    public void tokenSearchTest7() {
        utils.searchAll()
             .and("AND TOKEN(integer_1, ascii_1) > TOKEN(1, 'ascii')")
             .and("AND TOKEN(integer_1, ascii_1) <= TOKEN(3, 'ascii')")
             .check(4);
    }

    @Test
    public void tokenSearchTest8() {
        utils.searchAll()
             .and("AND TOKEN(integer_1, ascii_1) >= TOKEN(1, 'ascii')")
             .and("AND TOKEN(integer_1, ascii_1) <= TOKEN(3, 'ascii')")
             .check(5);
    }
}
