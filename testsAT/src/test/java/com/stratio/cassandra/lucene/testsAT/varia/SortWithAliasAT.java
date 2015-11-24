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

import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.stratio.cassandra.lucene.builder.Builder.*;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SortWithAliasAT extends BaseAT {

    private static CassandraUtils cassandraUtils;

    @BeforeClass
    public static void before() {

        cassandraUtils = CassandraUtils.builder("sort_alias")
                                       .withPartitionKey("key")
                                       .withColumn("key", "int")
                                       .withColumn("lucene", "text")
                                       .withColumn("text_1", "text", stringMapper().sorted(true))
                                       .withColumn("text_2", "text", null)
                                       .withColumn("map_1", "map<text, text>", null)
                                       .withMapper("alias_text_1", stringMapper().sorted(true).column("text_1"))
                                       .withMapper("alias_text_2", stringMapper().sorted(true).column("text_2"))
                                       .withMapper("alias_map_1", stringMapper().sorted(true).column("map_1"))
                                       .build()
                                       .createKeyspace()
                                       .createTable()
                                       .createIndex()
                                       .insert(new String[]{"key", "text_1", "text_2"}, new Object[]{1, "a", "l"})
                                       .insert(new String[]{"key", "text_1", "text_2"}, new Object[]{2, "b", "k"})
                                       .insert(new String[]{"key", "text_1", "text_2"}, new Object[]{3, "c", "j"})
                                       .insert(new String[]{"key", "text_1", "text_2"}, new Object[]{4, "d", "i"})
                                       .insert(new String[]{"key", "text_1", "text_2"}, new Object[]{5, "e", "h"})
                                       .insert(new String[]{"key", "text_1", "text_2"}, new Object[]{6, "f", "g"})
                                       .refresh();
    }

    @AfterClass
    public static void after() {
//        cassandraUtils.dropKeyspace();
    }

    @Test
    public void testSimpleSort() {
        cassandraUtils.select()
                      .sort(field("text_1"))
                      .checkStringColumn("text_1", "a", "b", "c", "d", "e", "f");
    }

    @Test
    public void testAlias1Sort() {
        cassandraUtils.select()
                      .sort(field("alias_text_1"))
                      .checkStringColumn("text_1", "g", "h", "i", "j", "k", "l");
    }

    @Test
    public void testAlias2Sort() {
        cassandraUtils.select()
                      .sort(field("alias_text_2"))
                      .checkStringColumn("text_2", "g", "h", "i", "j", "k", "l");
    }
}
