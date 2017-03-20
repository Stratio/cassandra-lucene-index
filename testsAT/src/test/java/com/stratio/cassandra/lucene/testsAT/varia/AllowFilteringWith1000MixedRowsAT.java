/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
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

@RunWith(JUnit4.class)
public class AllowFilteringWith1000MixedRowsAT extends BaseAT {

    private static CassandraUtils cassandraUtils;

    @BeforeClass
    public static void before() {
        cassandraUtils = CassandraUtils.builder("filtering_with_1000_mixed")
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
                                       .withColumn("lucene", "text")
                                       .build()
                                       .createKeyspace()
                                       .createTable()
                                       .createIndex();
        DataHelper.generateCustomInsertionsWithModule(1000, 4, cassandraUtils);
        cassandraUtils.refresh();
    }

    @AfterClass
    public static void after() {
        cassandraUtils.dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void allowFiltering1000rowsTest() {
        cassandraUtils.selectAllFromIndexQueryWithFiltering(1000, "double_1", 1D).check(250);
    }

    @Test
    public void allowFilteringAndLimit999Test() {
        cassandraUtils.selectAllFromIndexQueryWithFiltering(999, "double_1", 1D).check(250);
    }

    @Test
    public void allowFilteringAndLimit1001Test() {
        cassandraUtils.selectAllFromIndexQueryWithFiltering(1001, "double_1", 1D).check(250);
    }

    @Test
    public void allowFilteringAndLimit99Test() {
        cassandraUtils.selectAllFromIndexQueryWithFiltering(99, "double_1", 1D).check(99);
    }

    @Test
    public void allowFilteringAndLimit101Test() {
        cassandraUtils.selectAllFromIndexQueryWithFiltering(101, "double_1", 1D).check(101);
    }

    @Test
    public void allowFilteringAndLimit100Test() {
        cassandraUtils.selectAllFromIndexQueryWithFiltering(100, "double_1", 1D).check(100);
    }
}