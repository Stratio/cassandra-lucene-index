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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * Tests numeric mapping of date types (<a href="https://github.com/Stratio/cassandra-lucene-index/issues/177">issue
 * 177</a>)
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@RunWith(JUnit4.class)
public class Issue177IT extends BaseIT {

    @Test
    public void test() {
        builder("issue_177").withTable("test")
                            .withIndexName("idx")
                            .withColumn("id", "int")
                            .withColumn("a_date", "date")
                            .withColumn("a_timestamp", "timestamp")
                            .withIndexColumn("lucene")
                            .withPartitionKey("id")
                            .withMapper("a_date_millis", longMapper().column("a_date"))
                            .withMapper("a_timestamp_millis", longMapper().column("a_timestamp"))
                            .withMapper("a_date_days", integerMapper().column("a_date"))
                            .withMapper("a_timestamp_days", integerMapper().column("a_timestamp"))
                            .build()
                            .createKeyspace()
                            .createTable()
                            .createIndex()
                            .insert(new String[]{"id", "a_date", "a_timestamp"}, new Object[]{1, 1000, 1000})
                            .refresh()
                            .filter(match("a_timestamp_millis", 1000)).check(1)
                            .filter(match("a_date_days", 1000)).check(1)
                            .filter(match("a_timestamp_days", -2147483648)).check(1)
                            .filter(match("a_date_millis", -185542500787200000L)).check(1)
                            .dropKeyspace();
    }
}
