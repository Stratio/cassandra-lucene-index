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
package com.stratio.cassandra.lucene.testsAT.search;

import com.stratio.cassandra.lucene.builder.search.condition.Condition;
import com.stratio.cassandra.lucene.builder.search.sort.SortField;
import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsSelect;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static com.stratio.cassandra.lucene.builder.Builder.geoPointMapper;
import static com.stratio.cassandra.lucene.builder.Builder.stringMapper;
import static com.stratio.cassandra.lucene.testsAT.search.DataHelper.*;

public abstract class AbstractSearchAT extends BaseAT {

    static final String UNSUPPORTED_DOC_VALUES = "Field 'text_1' does not support doc_values";

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
                              .withColumn("text_2", "text", stringMapper())
                              .withColumn("varchar_1", "varchar")
                              .withColumn("uuid_1", "uuid")
                              .withColumn("timeuuid_1", "timeuuid")
                              .withColumn("list_1", "list<text>")
                              .withColumn("set_1", "set<text>")
                              .withColumn("map_1", "map<text,text>")
                              .withColumn("lat", "float")
                              .withColumn("long", "float")
                              .withMapper("geo_point", geoPointMapper("lat", "long"))
                              .withMapper("string_list", stringMapper().column("list_1"))
                              .withMapper("string_set", stringMapper().column("set_1"))
                              .withMapper("string_map", stringMapper().column("map_1"))
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

    protected CassandraUtilsSelect search() {
        return utils.search();
    }

    protected CassandraUtilsSelect filter(Condition... conditions) {
        return utils.filter(conditions);
    }

    protected CassandraUtilsSelect query(Condition... conditions) {
        return utils.query(conditions);
    }

    protected CassandraUtilsSelect sort(SortField... fields) {
        return utils.sort(fields);
    }
}
