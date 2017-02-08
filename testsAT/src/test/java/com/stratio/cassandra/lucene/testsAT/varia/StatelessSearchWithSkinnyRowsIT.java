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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.all;
import static com.stratio.cassandra.lucene.builder.Builder.field;

/**
 * Test fos repeated calls with the same search.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@RunWith(JUnit4.class)
public class StatelessSearchWithSkinnyRowsIT extends BaseIT {

    private static final int NUM_PARTITIONS = 100;

    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("stateless_search_skinny")
                              .withPartitionKey("pk")
                              .withColumn("pk", "int")
                              .withColumn("rc", "int")
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex();
        for (Integer i = 1; i <= NUM_PARTITIONS; i++) {
            Map<String, String> data = new LinkedHashMap<>();
            data.put("pk", i.toString());
            data.put("rc", i.toString());
            utils.insert(data);
        }
        utils.refresh();
    }

    @AfterClass
    public static void after() {
        utils.dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void testQuery() throws Exception {
        assertPure("Search with must is not a pure function",
                   () -> utils.query(all()).fetchSize(10).limit(40).getLast().get("rc", int.class));
    }

    @Test
    public void testFilter() throws Exception {
        assertPure("Search with filter is not a pure function",
                   () -> utils.filter(all()).fetchSize(1).limit(40).getLast().get("rc", int.class));
    }

    @Test
    public void testSort() throws Exception {
        assertPure("Search with sort is not a pure function",
                   () -> utils.sort(field("rc")).fetchSize(10).limit(40).getLast().get("rc", int.class));
    }
}
