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

package com.stratio.cassandra.lucene.deletion;

import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.BaseTest;
import com.stratio.cassandra.lucene.util.CassandraUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.wildcard;
import static com.stratio.cassandra.lucene.deletion.DataHelper.*;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class ComplexKeyDataDeletionTest  extends BaseTest {

    private CassandraUtils cassandraUtils;

    @Before
    public void before() {

        cassandraUtils = CassandraUtils.builder("complex_key_data_deletion")
                                       .withPartitionKey("integer_1", "ascii_1")
                                       .withClusteringKey("double_1")
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
                                               data20);
    }

    @After
    public void after() {
        cassandraUtils.dropKeyspace();
    }

    @Test
    public void columnDeletion() {

        List<Row> rows = cassandraUtils.delete("bigint_1")
                                       .where("integer_1", 1).and("ascii_1","ascii").and("double_1", 1)
                                       .refresh().filter(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 20 results!", 20, rows.size());

        int integerValue;
        String asciiValue;
        double doubleValue;
        for (Row row : rows) {
            integerValue = row.getInt("integer_1");
            asciiValue = row.getString("ascii_1");
            doubleValue = row.getDouble("double_1");
            if ((integerValue == 1) && (asciiValue.equals("ascii")) && (doubleValue == 1)) {
                assertTrue("Must be null!", row.isNull("bigint_1"));
            }
        }
    }

    @Test
    public void mapElementDeletion() {

        List<Row> rows = cassandraUtils.delete("map_1['k1']")
                                       .where("integer_1", 1).and("ascii_1", "ascii").and("double_1", 1)
                                       .refresh().filter(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 20 results!", 20, rows.size());

        int integerValue;
        String asciiValue;
        double doubleValue;
        Map<String, String> mapValue = null;
        for (Row row : rows) {
            integerValue = row.getInt("integer_1");
            asciiValue = row.getString("ascii_1");
            doubleValue = row.getDouble("double_1");
            if ((integerValue == 1) && (asciiValue.equals("ascii")) && (doubleValue == 1)) {
                mapValue = row.getMap("map_1", String.class, String.class);
            }
        }

        assertNotNull("Must not be null!", mapValue);
        assertNull("Must be null!", mapValue.get("k1"));
    }

    @Test
    public void listElementDeletion() {

        List<Row> rows = cassandraUtils.delete("list_1[0]")
                                       .where("integer_1", 1).and("ascii_1", "ascii").and("double_1", 1)
                                       .refresh().select().filter(wildcard("ascii_1", "*")).get();

        assertEquals("Expected 20 results!", 20, rows.size());

        int integerValue;
        String asciiValue;
        double doubleValue;
        List<String> listValue = null;
        for (Row row : rows) {
            integerValue = row.getInt("integer_1");
            asciiValue = row.getString("ascii_1");
            doubleValue = row.getDouble("double_1");
            if ((integerValue == 1) && (asciiValue.equals("ascii")) && (doubleValue == 1)) {
                listValue = row.getList("list_1", String.class);
            }
        }

        assertNotNull("Must not be null!", listValue);
        assertEquals("Length unexpected", 1, listValue.size());
    }

    @Test
    public void totalPartitionDeletion() {
        int n = cassandraUtils.delete().where("integer_1", 1).where("ascii_1", "ascii").where("double_1", 1)
                              .refresh().select().filter(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 19 results!", 19, n);

    }

    @Test
    public void partialPartitionDeletion() {
        int n = cassandraUtils.delete().where("integer_1", 1).and("ascii_1","ascii")
                              .refresh().select().filter(wildcard("ascii_1", "*")).count();
        assertEquals("Expected 18 results!", 18, n);
    }
}
