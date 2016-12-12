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

import com.google.common.collect.Sets;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.*;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * Test best effort mapping of collections (<a href="https://github.com/Stratio/cassandra-lucene-index/issues/246">issue
 * 246</a>).
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@RunWith(JUnit4.class)
public class Issue246IT extends BaseIT {

    @Test
    public void testSet() {
        Set<String> set = Sets.newHashSet("1", "a", "3", "999");
        builder("issue_246").withTable("test")
                            .withIndexName("idx")
                            .withColumn("id", "int")
                            .withColumn("value", "frozen<set<text>>")
                            .withIndexColumn("lucene")
                            .withPartitionKey("id")
                            .withMapper("value", integerMapper())
                            .build()
                            .createKeyspace()
                            .createTable()
                            .createIndex()
                            .insert(new String[]{"id", "value"}, new Object[]{1, set})
                            .refresh()
                            .filter(match("value", 1)).check(1)
                            .filter(match("value", 3)).check(1)
                            .filter(match("value", 999)).check(1)
                            .dropKeyspace();
    }

    @Test
    public void testList() {
        List<String> list = Arrays.asList("1", "a", "3", "999");
        builder("issue_246").withTable("test")
                            .withIndexName("idx")
                            .withColumn("id", "int")
                            .withColumn("value", "frozen<list<text>>")
                            .withIndexColumn("lucene")
                            .withPartitionKey("id")
                            .withMapper("value", integerMapper())
                            .build()
                            .createKeyspace()
                            .createTable()
                            .createIndex()
                            .insert(new String[]{"id", "value"}, new Object[]{1, list})
                            .refresh()
                            .filter(match("value", 1)).check(1)
                            .filter(match("value", 3)).check(1)
                            .filter(match("value", 999)).check(1)
                            .dropKeyspace();
    }

    @Test
    public void testMap() {
        Map<Integer, String> map = new HashMap<>();
        map.put(0, "1");
        map.put(1, "a");
        map.put(2, "3");
        map.put(3, "999");
        builder("issue_246").withTable("test")
                            .withIndexName("idx")
                            .withColumn("id", "int")
                            .withColumn("value", "frozen<map<int, text>>")
                            .withIndexColumn("lucene")
                            .withPartitionKey("id")
                            .withMapper("value", integerMapper())
                            .build()
                            .createKeyspace()
                            .createTable()
                            .createIndex()
                            .insert(new String[]{"id", "value"}, new Object[]{1, map})
                            .refresh()
                            .filter(match("value$0", 1)).check(1)
                            .filter(match("value$2", 3)).check(1)
                            .filter(match("value$3", 999)).check(1)
                            .dropKeyspace();
    }
}
