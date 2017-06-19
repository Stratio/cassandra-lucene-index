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
package com.stratio.cassandra.lucene.testsAT.schema.analysis;

import com.stratio.cassandra.lucene.builder.index.schema.analysis.tokenizer.NGramTokenizer;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static com.stratio.cassandra.lucene.builder.Builder.match;

/**
 * Test Custom Analyzer.
 *
 * @author Juan Pedro Gilaberte {@literal <jpgilaberte@stratio.com>}
 */
@RunWith(JUnit4.class)
public class CustomAnalyzerIT extends BaseIT {
    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {}

    @AfterClass
    public static void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    @Test
    public void testCustomAnalyzer() {
        utils = CassandraUtils.builder("tokenizer")
                .withPartitionKey("pk")
                .withColumn("pk", "int")
                .withColumn("rc", "text", textMapper().analyzer("en"))
                .withAnalyzer("en", customAnalyzer(new NGramTokenizer(2,2)))
                .build()
                .createKeyspace()
                .createTable()
                .insert("pk,rc", 1, "aabb")
                .createIndex().refresh()
                .filter(all()).check(1)
                .filter(none()).check(0)
                .filter(match("rc", "aa")).check(1)
                .filter(match("rc", "ab")).check(1)
                .filter(match("rc", "bb")).check(1);
    }
}