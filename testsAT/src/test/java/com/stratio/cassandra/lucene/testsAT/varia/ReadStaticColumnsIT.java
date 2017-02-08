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
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsSelect;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.stratio.cassandra.lucene.builder.Builder.match;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class ReadStaticColumnsIT extends BaseIT {

    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("read_static_columns")
                              .withPartitionKey("key")
                              .withClusteringKey("cluster_key")
                              .withColumn("key", "bigint")
                              .withColumn("cluster_key", "int")
                              .withStaticColumn("name", "text", false)
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex()
                              .insert(new String[]{"key", "cluster_key", "name"},
                                      new Object[]{12, 13, "Name12"})
                              .insert(new String[]{"key", "cluster_key", "name"},
                                      new Object[]{12, 14, "Name12-2"})
                              .insert(new String[]{"key", "cluster_key", "name"},
                                      new Object[]{15, 16, "Name15"})
                              .insert(new String[]{"key", "cluster_key", "name"},
                                      new Object[]{15, 17, "Name15-2"})
                              .refresh();

    }

    @AfterClass
    public static void after() {
        utils.dropKeyspace();
    }

    @Test
    public void testTokenReadStaticColumn1() {
        CassandraUtilsSelect select = utils.filter(match("key", 12));
        select.checkUnorderedColumns("key", 12L, 12L);
        select.checkUnorderedColumns("cluster_key", 13, 14);
        select.checkUnorderedColumns("name", "Name12-2", "Name12-2");
    }

    @Test
    public void testTokenReadStaticColumn2() {
        CassandraUtilsSelect select = utils.filter(match("key", 15));
        select.checkUnorderedColumns("key", 15L, 15L);
        select.checkUnorderedColumns("cluster_key", 16, 17);
        select.checkUnorderedColumns("name", "Name15-2", "Name15-2");
    }
}