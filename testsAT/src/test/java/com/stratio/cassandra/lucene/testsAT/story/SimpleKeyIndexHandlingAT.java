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

package com.stratio.cassandra.lucene.testsAT.story;

import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.wildcard;
import static com.stratio.cassandra.lucene.testsAT.story.DataHelper.*;

@RunWith(JUnit4.class)
public class SimpleKeyIndexHandlingAT extends BaseAT {

    private CassandraUtils cassandraUtils;

    @Before
    public void before() {
        cassandraUtils = CassandraUtils.builder("simple_key_index_handling")
                                       .withPartitionKey("integer_1")
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
                                       .createTable();
    }

    @After
    public void after() {
        cassandraUtils.dropTable().dropKeyspace();
    }

    @Test
    public void createIndexAfterInsertionsTest() {
        cassandraUtils.insert(data1, data2, data3, data4, data5)
                      .createIndex()
                      .waitForIndexing()
                      .refresh()
                      .query(wildcard("ascii_1", "*"))
                      .check(5);
    }

    @Test
    public void createIndexDuringInsertionsTest() {
        cassandraUtils.insert(data1, data2, data3)
                      .createIndex()
                      .waitForIndexing()
                      .insert(data4, data5)
                      .refresh()
                      .query(wildcard("ascii_1", "*"))
                      .check(5);
    }

    @Test
    public void recreateIndexAfterInsertionsTest() {
        cassandraUtils.createIndex()
                      .waitForIndexing()
                      .insert(data1, data2, data3, data4, data5)
                      .refresh()
                      .query(wildcard("ascii_1", "*"))
                      .check(5)
                      .dropIndex()
                      .createIndex()
                      .waitForIndexing()
                      .refresh()
                      .query(wildcard("ascii_1", "*"))
                      .check(5);
    }
}