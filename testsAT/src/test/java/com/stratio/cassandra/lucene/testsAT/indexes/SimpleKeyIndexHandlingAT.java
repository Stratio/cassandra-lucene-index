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
package com.stratio.cassandra.lucene.testsAT.indexes;

import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.wildcard;
import static com.stratio.cassandra.lucene.testsAT.indexes.DataHelper.*;

@RunWith(JUnit4.class)
public class SimpleKeyIndexHandlingAT extends BaseAT {

    private CassandraUtils utils;

    @Before
    public void setUpTest() {
        utils = CassandraUtils.builder("simple_key_index_handling")
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
                              .createTable();
    }

    @After
    public void tearDown() {
        utils.dropTable().dropKeyspace();
    }

    @Test
    public void createIndexAfterInsertionsTest() {
        utils.insert(data1, data2, data3, data4, data5)
             .createIndex()
             .refresh()
             .filter(wildcard("ascii_1", "*"))
             .check(5);
    }

    @Test
    public void createIndexDuringInsertionsTest() {
        utils.insert(data1, data2, data3)
             .createIndex()
             .insert(data4, data5)
             .refresh()
             .filter(wildcard("ascii_1", "*"))
             .check(5);
    }

    @Test
    public void recreateIndexAfterInsertionsTest() {
        utils.createIndex()
             .insert(data1, data2, data3, data4, data5)
             .refresh()
             .filter(wildcard("ascii_1", "*"))
             .check(5)
             .dropIndex()
             .createIndex()
             .refresh()
             .filter(wildcard("ascii_1", "*"))
             .check(5);
    }
}