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
package com.stratio.cassandra.lucene.testsAT.varia;

import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.stratio.cassandra.lucene.builder.Builder.*;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class MultiMappingIT extends BaseIT {

    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("sort_alias")
                              .withPartitionKey("key")
                              .withColumn("key", "int")
                              .withColumn("text", "text", stringMapper())
                              .withColumn("map", "map<text, text>", null)
                              .withMapper("alias_text", dateMapper().pattern("dd-MM-yyyy").column("text"))
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex()
                              .insert(new String[]{"key", "text"}, new Object[]{1, "01-01-2014"})
                              .insert(new String[]{"key", "text"}, new Object[]{2, "02-01-2013"})
                              .insert(new String[]{"key", "text"}, new Object[]{3, "03-01-2012"})
                              .insert(new String[]{"key", "text"}, new Object[]{4, "04-01-2011"})
                              .refresh();
    }

    @AfterClass
    public static void after() {
        utils.dropKeyspace();
    }

    @Test
    public void testSimpleQuery() {
        utils.query(match("text", "02-01-2013")).check(1);
    }

    @Test
    public void testAliasQuery() {
        utils.query(match("alias_text", "02-01-2013")).check(1);
    }

    @Test
    public void testSimpleFilter() {
        utils.filter(match("text", "02-01-2013")).check(1);
    }

    @Test
    public void testAliasFilter() {
        utils.filter(match("alias_text", "02-01-2013")).check(1);
    }

    @Test
    public void testSimpleSort() {
        utils.select()
             .sort(field("text"))
             .checkOrderedColumns("text", "01-01-2014", "02-01-2013", "03-01-2012", "04-01-2011");
    }

    @Test
    public void testAliasSort() {
        utils.select()
             .sort(field("alias_text"))
             .checkOrderedColumns("text", "04-01-2011", "03-01-2012", "02-01-2013", "01-01-2014");
    }
}
