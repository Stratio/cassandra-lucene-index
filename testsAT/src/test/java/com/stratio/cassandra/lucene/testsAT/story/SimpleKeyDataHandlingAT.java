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

import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static com.stratio.cassandra.lucene.builder.Builder.wildcard;
import static com.stratio.cassandra.lucene.testsAT.story.DataHelper.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(JUnit4.class)
public class SimpleKeyDataHandlingAT extends BaseAT {

    private CassandraUtils cassandraUtils;

    @Before
    public void before() {
        cassandraUtils = CassandraUtils.builder("simple_key_data_handling")
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
                                       .createTable()
                                       .createIndex()
                                       .insert(data1, data2, data3)
                                       .refresh();
    }

    @After
    public void after() {
        cassandraUtils.dropTable().dropKeyspace();
    }

    @Test
    public void singleInsertion() {

        // Data4 insertion
        cassandraUtils.insert(data4).refresh();

        List<Row> rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 4 results!", 4, rows.size());

        // Data5 insertion
        cassandraUtils.insert(data5).refresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 5 results!", 5, rows.size());

        // Data4 removal
        cassandraUtils.delete().where("integer_1", 4).refresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 4 results!", 4, rows.size());
        assertFalse("Element not expected!", containsElementByIntegerKey(rows, 4));

        // Data5 removal
        cassandraUtils.delete().where("integer_1", 5).refresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 3 results!", 3, rows.size());
        assertFalse("Element not expected!", containsElementByIntegerKey(rows, 5));

        // Data2 removal
        cassandraUtils.delete().where("integer_1", 2).refresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 2 results!", 2, rows.size());
        assertFalse("Element not expected!", containsElementByIntegerKey(rows, 2));

        // Data3 removal
        cassandraUtils.delete().where("integer_1", 3).refresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 1 result!", 1, rows.size());
        assertFalse("Element not expected!", containsElementByIntegerKey(rows, 3));

        // Data1 removal
        cassandraUtils.delete().where("integer_1", 1).refresh();

        rows = cassandraUtils.query(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 0 results!", 0, rows.size());
        assertFalse("Element not expected!", containsElementByIntegerKey(rows, 1));
    }

    @Test
    public void multipleInsertion() {

        // Data4 and data5 insertion
        List<Row> rows = cassandraUtils.insert(data4, data5).refresh().query(wildcard("ascii_1", "*")).get();
        assertEquals("Expected 5 results!", 5, rows.size());

        // Data4 removal
        rows = cassandraUtils.delete().where("integer_1", 4).refresh().query(wildcard("ascii_1", "*")).get();
        assertEquals("Expected 4 results!", 4, rows.size());
        assertFalse("Element not expected!", containsElementByIntegerKey(rows, 4));

        // Data5 removal
        rows = cassandraUtils.delete().where("integer_1", 5).refresh().query(wildcard("ascii_1", "*")).get();
        assertEquals("Expected 3 results!", 3, rows.size());
        assertFalse("Element not expected!", containsElementByIntegerKey(rows, 5));

        // Data2 removal
        rows = cassandraUtils.delete().where("integer_1", 2).refresh().query(wildcard("ascii_1", "*")).get();
        assertEquals("Expected 2 results!", 2, rows.size());
        assertFalse("Element not expected!", containsElementByIntegerKey(rows, 2));

        // Data3 removal
        rows = cassandraUtils.delete().where("integer_1", 3).refresh().query(wildcard("ascii_1", "*")).get();
        assertEquals("Expected 1 result!", 1, rows.size());
        assertFalse("Element not expected!", containsElementByIntegerKey(rows, 3));

        // Data1 removal
        rows = cassandraUtils.delete().where("integer_1", 1).refresh().query(wildcard("ascii_1", "*")).get();
        assertEquals("Expected 0 results!", 0, rows.size());
        assertFalse("Element not expected!", containsElementByIntegerKey(rows, 1));
    }

    @Test
    public void multipleDeletion() {
        List<Row> rows = cassandraUtils.delete()
                                       .where("integer_1", 2)
                                       .delete()
                                       .where("integer_1", 3)
                                       .refresh()
                                       .query(wildcard("ascii_1", "*"))
                                       .get();
        assertEquals("Expected 1 result!", 1, rows.size());
        assertFalse("Element not expected!", containsElementByIntegerKey(rows, 3));

        rows = cassandraUtils.delete().where("integer_1", 1).refresh().query(wildcard("ascii_1", "*")).get();
        assertEquals("Expected 0 results!", 0, rows.size());
        assertFalse("Element not expected!", containsElementByIntegerKey(rows, 1));
    }

    @Test
    public void updateTest() {
        cassandraUtils.query(wildcard("text_1", "text"))
                      .check(3)
                      .update()
                      .set("text_1", "other")
                      .where("integer_1", 2)
                      .refresh()
                      .query(wildcard("text_1", "text"))
                      .check(2)
                      .query(wildcard("text_1", "other"))
                      .check(1);
    }

    @Test
    public void insertWithUpdateTest() {
        cassandraUtils.query(wildcard("text_1", "text"))
                      .check(3)
                      .update()
                      .set("text_1", "new")
                      .where("integer_1", 1000)
                      .refresh()
                      .query(wildcard("text_1", "new"))
                      .check(1);
    }

    private static boolean containsElementByIntegerKey(List<Row> resultList, int key) {

        boolean isContained = false;
        int elementKey;
        for (Row resultElement : resultList) {
            elementKey = resultElement.getInt("integer_1");

            if (elementKey == key) {
                isContained = true;
            }
        }

        return isContained;
    }
}
