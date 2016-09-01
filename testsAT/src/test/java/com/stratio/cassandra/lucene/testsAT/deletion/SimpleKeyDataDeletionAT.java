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
package com.stratio.cassandra.lucene.testsAT.deletion;

import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.wildcard;
import static com.stratio.cassandra.lucene.testsAT.deletion.DataHelper.*;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class SimpleKeyDataDeletionAT extends BaseAT {

    private CassandraUtils utils;

    @Before
    public void before() {
        utils = CassandraUtils.builder("simple_key_data_deletion")
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
                              .createIndex()
                              .insert(data1, data2, data3, data4, data5);
    }

    @After
    public void after() {
        utils.dropTable().dropKeyspace();
    }

    @Test
    public void columnDeletion() {

        List<Row> rows = utils.delete("bigint_1")
                              .where("integer_1", 1)
                              .refresh()
                              .filter(wildcard("ascii_1", "*"))
                              .get();

        assertEquals("Expected 5 results!", 5, rows.size());

        int integerValue;
        for (Row row : rows) {
            integerValue = row.getInt("integer_1");
            if (integerValue == 1) {
                assertTrue("Must be null!", row.isNull("bigint_1"));
            }
        }
    }

    @Test
    public void mapElementDeletion() {

        List<Row> rows = utils.delete("map_1['k1']")
                              .where("integer_1", 1)
                              .refresh()
                              .filter(wildcard("ascii_1", "*"))
                              .get();

        assertEquals("Expected 5 results!", 5, rows.size());

        int integerValue;
        Map<String, String> mapValue = null;
        for (Row row : rows) {
            integerValue = row.getInt("integer_1");
            if (integerValue == 1) {
                mapValue = row.getMap("map_1", String.class, String.class);
            }
        }

        assertNotNull("Must not be null!", mapValue);
        assertNull("Must be null!", mapValue.get("k1"));
    }

    @Test
    public void listElementDeletion() {

        List<Row> rows = utils.delete("list_1[0]")
                              .where("integer_1", 1)
                              .refresh()
                              .filter(wildcard("ascii_1", "*"))
                              .get();

        assertEquals("Expected 5 results!", 5, rows.size());

        int integerValue;
        List<String> listValue = null;
        for (Row row : rows) {
            integerValue = row.getInt("integer_1");
            if (integerValue == 1) {
                listValue = row.getList("list_1", String.class);
            }
        }

        assertNotNull("Must not be null!", listValue);
        assertEquals("Length unexpected", 1, listValue.size());
    }

    @Test
    public void totalPartitionDeletion() {
        utils.delete().where("integer_1", 1).refresh().filter(wildcard("ascii_1", "*")).check(4);
    }
}
