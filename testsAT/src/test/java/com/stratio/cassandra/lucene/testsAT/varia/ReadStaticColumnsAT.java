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

package com.stratio.cassandra.lucene.testsAT.varia;

import com.datastax.driver.core.Row;
import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsSelect;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.stratio.cassandra.lucene.builder.Builder.match;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class ReadStaticColumnsAT extends BaseAT {

    private static CassandraUtils cassandraUtils;
    @BeforeClass
    public static void before() {
        cassandraUtils = CassandraUtils.builder("read_static_columns")
                                       .withPartitionKey("key")
                                       .withClusteringKey("cluster_key")
                                       .withColumn("key", "bigint")
                                       .withColumn("cluster_key", "int")
                                       .withStaticColumn("name", "text", false)
                                       .withColumn("lucene", "text")
                                       .build()
                                       .createKeyspace()
                                       .createTable()
                                       .createIndex()
                                       .insert(new String[]{"key", "cluster_key", "name"}, new Object[]{12, 13, "Name12"})
                                       .insert(new String[]{"key", "cluster_key", "name"}, new Object[]{12, 14, "Name12-2"})
                                       .insert(new String[]{"key", "cluster_key", "name"}, new Object[]{15, 16, "Name15"})
                                       .insert(new String[]{"key", "cluster_key", "name"}, new Object[]{15, 17, "Name15-2"})
                                       .refresh();

    }


    @AfterClass
    public static void after() {
        cassandraUtils.dropKeyspace();
    }

    @Test
    public void tokenReadStaticColumnTest1() {

        CassandraUtilsSelect select =cassandraUtils.filter(match("key",12));

        List<Row> rows= select.get();


        assertEquals("Expected 2 rows",2,rows.size());
        for (Row row: rows) {
            Long key= row.getLong("key");
            Integer cluster_key= row.getInt("cluster_key");
            String name = row.getString("name");
            assertEquals("Unexpected result!","12",key.toString());
            assertEquals("Unexpected result!","Name12-2",name);
            assertTrue("Expected cluster_key 13 or 14 ",(cluster_key.equals(13)  || (cluster_key.equals(14))));
        }

    }

    @Test
    public void tokenReadStaticColumnTest2() {

        List<Row> rows= cassandraUtils.filter(match("key",15)).get();
        assertEquals("Expected 2 rows",2,rows.size());
        for (Row row: rows) {
            Long key= row.getLong("key");
            Integer cluster_key= row.getInt("cluster_key");
            String name = row.getString("name");
            assertEquals("Unexpected result!","15",key.toString());
            assertEquals("Unexpected result!","Name15-2",name);
            assertTrue("Expected cluster_key 16 or 17 ",(cluster_key.equals(16)  || (cluster_key.equals(17))));
        }

    }

}
