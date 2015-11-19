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

package com.stratio.cassandra.lucene.testsAT.search;

import com.stratio.cassandra.lucene.builder.search.condition.Condition;
import com.stratio.cassandra.lucene.builder.search.sort.SortField;
import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsSelect;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static com.stratio.cassandra.lucene.testsAT.search.DataHelper.*;

public abstract class AbstractSearchAT extends BaseAT {

    protected static CassandraUtils utils;

    @BeforeClass
    public static void beforeClass() {
        utils = CassandraUtils.builder("search")
                              .withPartitionKey("integer_1", "double_1")
                              .withClusteringKey("ascii_1")
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
                              .insert(data1, data2, data3, data4, data5)
                              .refresh();
    }

    @AfterClass
    public static void afterClass() {
        utils.dropKeyspace();
    }

    protected CassandraUtilsSelect filter(Condition condition) {
        return utils.filter(condition);
    }

    protected CassandraUtilsSelect query(Condition condition) {
        return utils.query(condition);
    }

    protected CassandraUtilsSelect sort(SortField... sorts) {
        return utils.sort(sorts);
    }
}
