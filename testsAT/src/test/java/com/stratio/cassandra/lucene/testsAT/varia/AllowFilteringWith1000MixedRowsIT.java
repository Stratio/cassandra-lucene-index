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

@RunWith(JUnit4.class)
public class AllowFilteringWith1000MixedRowsIT extends BaseIT {

    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("filtering_with_1000_mixed")
                              .withPartitionKey("integer_1")
                              .withClusteringKey()
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
                              .createIndex();
        DataHelper.generateCustomInsertionsWithModule(1000, 4, utils);
        utils.refresh();
    }

    @AfterClass
    public static void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    @Test
    public void testAllowFiltering1000rows() {
        utils.searchAllWithFiltering(1000, "double_1", 1D).check(250);
    }

    @Test
    public void testAllowFilteringAndLimit999() {
        utils.searchAllWithFiltering(999, "double_1", 1D).check(250);
    }

    @Test
    public void testAllowFilteringAndLimit1001() {
        utils.searchAllWithFiltering(1001, "double_1", 1D).check(250);
    }

    @Test
    public void testAllowFilteringAndLimit99() {
        utils.searchAllWithFiltering(99, "double_1", 1D).check(99);
    }

    @Test
    public void testAllowFilteringAndLimit101() {
        utils.searchAllWithFiltering(101, "double_1", 1D).check(101);
    }

    @Test
    public void testAllowFilteringAndLimit100() {
        utils.searchAllWithFiltering(100, "double_1", 1D).check(100);
    }
}