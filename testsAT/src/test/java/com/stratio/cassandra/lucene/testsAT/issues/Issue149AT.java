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

import com.stratio.cassandra.lucene.testsAT.BaseAT;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.integerMapper;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * Test deletion of columns with frozen type (UDT, tuples and frozen collections) (<a
 * href="https://github.com/Stratio/cassandra-lucene-index/issues/149">issue 149</a>).
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@RunWith(JUnit4.class)
public class Issue149AT extends BaseAT {

    @Test
    public void testSkinny() {
        builder("issue_149").withTable("test_skinny")
                            .withIndexName("idx")
                            .withUDT("uct", "a", "int")
                            .withUDT("uct", "b", "int")
                            .withColumn("pk", "int", null)
                            .withColumn("rc", "int")
                            .withColumn("uc", "frozen<uct>", null)
                            .withColumn("tc", "tuple<int,int>", null)
                            .withColumn("sc", "frozen<set<int>>")
                            .withColumn("lc", "frozen<list<int>>")
                            .withColumn("mc", "frozen<map<int,int>>")
                            .withMapper("tc.0", integerMapper())
                            .withMapper("tc.1", integerMapper())
                            .withMapper("uc.a", integerMapper())
                            .withMapper("uc.b", integerMapper())
                            .withPartitionKey("pk")
                            .build()
                            .createKeyspace()
                            .createUDTs()
                            .createTable()
                            .createIndex()
                            .insert(new String[]{"pk", "rc"}, new Object[]{1, null})
                            .insert(new String[]{"pk", "tc"}, new Object[]{1, null})
                            .insert(new String[]{"pk", "uc"}, new Object[]{1, null})
                            .insert(new String[]{"pk", "sc"}, new Object[]{1, null})
                            .insert(new String[]{"pk", "lc"}, new Object[]{1, null})
                            .insert(new String[]{"pk", "mc"}, new Object[]{1, null})
                            .refresh()
                            .searchAll()
                            .check(0)
                            .dropIndex()
                            .createIndex()
                            .refresh()
                            .searchAll()
                            .check(0)
                            .insert(new String[]{"pk", "rc"}, new Object[]{1, 1})
                            .insert(new String[]{"pk", "tc"}, new Object[]{1, null})
                            .insert(new String[]{"pk", "uc"}, new Object[]{1, null})
                            .insert(new String[]{"pk", "sc"}, new Object[]{1, null})
                            .insert(new String[]{"pk", "lc"}, new Object[]{1, null})
                            .insert(new String[]{"pk", "mc"}, new Object[]{1, null})
                            .refresh()
                            .searchAll()
                            .check(1)
                            .dropIndex()
                            .createIndex()
                            .refresh()
                            .searchAll()
                            .check(1)
                            .dropKeyspace();
    }

    @Test
    public void testWide() {
        builder("issue_149").withTable("test_wide")
                            .withIndexName("idx")
                            .withUDT("uct", "a", "int")
                            .withUDT("uct", "b", "int")
                            .withColumn("pk", "int", null)
                            .withColumn("ck", "int", null)
                            .withColumn("rc", "int")
                            .withColumn("uc", "frozen<uct>", null)
                            .withColumn("tc", "tuple<int,int>", null)
                            .withColumn("sc", "frozen<set<int>>")
                            .withColumn("lc", "frozen<list<int>>")
                            .withColumn("mc", "frozen<map<int,int>>")
                            .withMapper("tc.0", integerMapper())
                            .withMapper("tc.1", integerMapper())
                            .withMapper("uc.a", integerMapper())
                            .withMapper("uc.b", integerMapper())
                            .withPartitionKey("pk")
                            .withClusteringKey("ck")
                            .build()
                            .createKeyspace()
                            .createUDTs()
                            .createTable()
                            .createIndex()
                            .insert(new String[]{"pk", "ck", "rc"}, new Object[]{1, 2, null})
                            .insert(new String[]{"pk", "ck", "tc"}, new Object[]{1, 2, null})
                            .insert(new String[]{"pk", "ck", "uc"}, new Object[]{1, 2, null})
                            .insert(new String[]{"pk", "ck", "sc"}, new Object[]{1, 2, null})
                            .insert(new String[]{"pk", "ck", "lc"}, new Object[]{1, 2, null})
                            .insert(new String[]{"pk", "ck", "mc"}, new Object[]{1, 2, null})
                            .refresh()
                            .searchAll()
                            .check(0)
                            .dropIndex()
                            .createIndex()
                            .refresh()
                            .searchAll()
                            .check(0)
                            .insert(new String[]{"pk", "ck", "rc"}, new Object[]{1, 2, 1})
                            .insert(new String[]{"pk", "ck", "tc"}, new Object[]{1, 2, null})
                            .insert(new String[]{"pk", "ck", "uc"}, new Object[]{1, 2, null})
                            .insert(new String[]{"pk", "ck", "sc"}, new Object[]{1, 2, null})
                            .insert(new String[]{"pk", "ck", "lc"}, new Object[]{1, 2, null})
                            .insert(new String[]{"pk", "ck", "mc"}, new Object[]{1, 2, null})
                            .refresh()
                            .searchAll()
                            .check(1)
                            .dropIndex()
                            .createIndex()
                            .refresh()
                            .searchAll()
                            .check(1)
                            .dropKeyspace();
    }
}
