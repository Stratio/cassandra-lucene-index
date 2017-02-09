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
package com.stratio.cassandra.lucene.testsAT.issues;

import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.match;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class Issue123IT extends BaseIT {

    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("issue_123")
                              .withPartitionKey("partition")
                              .withClusteringKey("id")
                              .withColumn("partition", "int")
                              .withColumn("id", "int")
                              .withColumn("ascii_1", "ascii")
                              .withColumn("bigint_1", "bigint")
                              .withColumn("blob_1", "blob")
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex();
        for (Integer p = 0; p < 2; p++) {
            for (Integer i = 1; i <= 100; i++) {
                Map<String, String> data = new LinkedHashMap<>();
                data.put("partition", p.toString());
                data.put("id", i.toString());
                data.put("ascii_1", "'ascii_bis'");
                data.put("bigint_1", "3000000000000000");
                data.put("blob_1", "0x3E0A15");
                utils.insert(data);
            }
        }
        utils.refresh();
    }

    @Test
    public void testQueryWithFetchSizeToIntegerMaxValue() {
        utils.query(match("partition", 0)).fetchSize(Integer.MAX_VALUE).check(100);
    }

    @AfterClass
    public static void after() {
        utils.dropIndex().dropTable().dropKeyspace();
    }
}
