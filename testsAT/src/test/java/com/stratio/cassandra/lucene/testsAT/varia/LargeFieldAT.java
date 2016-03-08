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

import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import static com.stratio.cassandra.lucene.builder.Builder.bool;
import static com.stratio.cassandra.lucene.builder.Builder.match;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class LargeFieldAT {

    @Test
    public void testLargeField() throws IOException {

        CassandraUtils cassandraUtils = CassandraUtils.builder("large_field")
                                                      .withPartitionKey("id")
                                                      .withClusteringKey("name", "age")
                                                      .withColumn("id", "varchar")
                                                      .withColumn("name", "varchar")
                                                      .withColumn("age", "varchar")
                                                      .withColumn("data", "varchar")
                                                      .withColumn("lucene", "varchar")
                                                      .build()
                                                      .createKeyspace()
                                                      .createTable()
                                                      .createIndex();

        int numNumbers = 5000;
        UUID[] numbers = new UUID[numNumbers];
        for (int i = 0; i < numNumbers; i++) {
            numbers[i] = UUID.randomUUID();
        }
        String largeString = Arrays.toString(numbers);

        cassandraUtils.insert(new String[]{"id", "name", "age", "data"}, new Object[]{"2", "b", "2", "good_dat"})
                      .insert(new String[]{"id", "name", "age", "data"}, new Object[]{"1", "a", "1", largeString})
                      .refresh()
                      .query(bool().must(match("id", "2")).must(match("name", "b")))
                      .check(1)
                      .query(bool().must(match("id", "1")).must(match("name", "a")))
                      .check(1)
                      .dropKeyspace();
    }
}
