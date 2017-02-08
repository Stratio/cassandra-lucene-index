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
package com.stratio.cassandra.lucene.testsAT.story;

import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.wildcard;
import static com.stratio.cassandra.lucene.testsAT.story.DataHelper.*;

@RunWith(JUnit4.class)
public class SimpleKeyDataHandlingIT extends BaseIT {

    private CassandraUtils utils;

    @Before
    public void before() {
        utils = CassandraUtils.builder("simple_key_data_handling")
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
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex()
                              .insert(data1, data2, data3)
                              .refresh();
    }

    @After
    public void after() {
        utils.dropTable().dropKeyspace();
    }

    @Test
    public void testSingleInsertion() {
        // Data4 insertion
        utils.insert(data4).refresh()
             .query(wildcard("ascii_1", "*")).check(4)
             .insert(data5).refresh()
             .query(wildcard("ascii_1", "*")).check(5)
             .delete().where("integer_1", 4).refresh()
             .query(wildcard("ascii_1", "*")).checkUnorderedColumns("integer_1", 1, 2, 3, 5)
             .delete().where("integer_1", 5).refresh()
             .query(wildcard("ascii_1", "*")).checkUnorderedColumns("integer_1", 1, 2, 3)
             .delete().where("integer_1", 2).refresh()
             .query(wildcard("ascii_1", "*")).checkUnorderedColumns("integer_1", 1, 3)
             .delete().where("integer_1", 3).refresh()
             .query(wildcard("ascii_1", "*")).checkUnorderedColumns("integer_1", 1)
             .delete().where("integer_1", 1).refresh()
             .query(wildcard("ascii_1", "*")).check(0);
    }

    @Test
    public void testMultipleInsertion() {
        // Data4 and data5 insertion
        utils.insert(data4, data5).refresh()
             .query(wildcard("ascii_1", "*")).check(5)
             .delete().where("integer_1", 4).refresh()
             .query(wildcard("ascii_1", "*")).checkUnorderedColumns("integer_1", 1, 2, 3, 5)
             .delete().where("integer_1", 5).refresh()
             .query(wildcard("ascii_1", "*")).checkUnorderedColumns("integer_1", 1, 2, 3)
             .delete().where("integer_1", 2).refresh()
             .query(wildcard("ascii_1", "*")).checkUnorderedColumns("integer_1", 1, 3)
             .delete().where("integer_1", 3).refresh()
             .query(wildcard("ascii_1", "*")).checkUnorderedColumns("integer_1", 1)
             .delete().where("integer_1", 1).refresh()
             .query(wildcard("ascii_1", "*")).check(0);
    }

    @Test
    public void testMultipleDeletion() {
        utils.delete().where("integer_1", 2)
             .delete().where("integer_1", 3).refresh()
             .query(wildcard("ascii_1", "*")).checkUnorderedColumns("integer_1", 1)
             .delete().where("integer_1", 1).refresh()
             .query(wildcard("ascii_1", "*")).check(0);
    }

    @Test
    public void testUpdate() {
        utils.query(wildcard("text_1", "text")).check(3)
             .update().set("text_1", "other").where("integer_1", 2).refresh()
             .query(wildcard("text_1", "text")).check(2)
             .query(wildcard("text_1", "other")).check(1);
    }

    @Test
    public void testInsertWithUpdate() {
        utils.query(wildcard("text_1", "text")).check(3)
             .update().set("text_1", "new").where("integer_1", 1000).refresh()
             .query(wildcard("text_1", "new")).check(1);
    }
}
