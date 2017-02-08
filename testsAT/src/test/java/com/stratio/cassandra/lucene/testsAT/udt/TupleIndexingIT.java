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
package com.stratio.cassandra.lucene.testsAT.udt;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TupleType;
import com.stratio.cassandra.lucene.builder.Builder;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.datastax.driver.core.DataType.*;
import static com.stratio.cassandra.lucene.builder.Builder.*;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */

@RunWith(JUnit4.class)
public class TupleIndexingIT extends BaseIT {

    private static CassandraUtils utils;

    @BeforeClass
    public static void before() {
        TupleType tuple = TupleType.of(ProtocolVersion.NEWEST_SUPPORTED,
                                       CodecRegistry.DEFAULT_INSTANCE,
                                       cint(),
                                       text(),
                                       cfloat());
        utils = CassandraUtils.builder("tuple_indexing")
                              .withColumn("k", "int")
                              .withColumn("v", "tuple<int, text, float>")
                              .withPartitionKey("k")
                              .withMapper("v.0", integerMapper())
                              .withMapper("v.1", stringMapper())
                              .withMapper("v.2", floatMapper())
                              .build()
                              .createKeyspace()
                              .createTable()
                              .createIndex()
                              .insert(new String[]{"k", "v"}, new Object[]{0, tuple.newValue(1, "foo", 2.1f)})
                              .insert(new String[]{"k", "v"}, new Object[]{1, tuple.newValue(2, "bar", 2.2f)})
                              .insert(new String[]{"k", "v"}, new Object[]{2, tuple.newValue(3, "zas", 1.2f)})
                              .refresh();
    }

    @AfterClass
    public static void after() {
        utils.dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void testSearchTuple1() {
        utils.filter(match("v.0", 1)).check(1)
             .filter(match("v.0", 2)).check(1)
             .filter(match("v.0", 3)).check(1)
             .filter(match("v.0", 4)).check(0)
             .filter(range("v.0").lower(1).includeLower(true).upper(2).includeUpper(true)).check(2)
             .filter(range("v.0").lower(2).includeLower(true).upper(3).includeUpper(true)).check(2)
             .filter(range("v.0").lower(3).includeLower(true).upper(4).includeUpper(true)).check(1)
             .filter(range("v.0").lower(4).includeLower(true).upper(5).includeUpper(true)).check(0)
             .sort(field("v.0").reverse(true)).checkOrderedColumns("k", 2, 1, 0);
    }

    @Test
    public void testSearchTuple2() {
        utils.filter(match("v.1", "foo")).checkUnorderedColumns("k", 0)
             .filter(match("v.1", "bar")).checkUnorderedColumns("k", 1)
             .filter(match("v.1", "zas")).checkUnorderedColumns("k", 2)
             .sort(field("v.1")).checkOrderedColumns("k", 1, 0, 2);
    }

    @Test
    public void testSearchTuple3() {
        utils.filter(match("v.2", 2.1)).checkUnorderedColumns("k", 0)
             .filter(match("v.2", 2.2)).checkUnorderedColumns("k", 1)
             .filter(match("v.2", 1.2)).checkUnorderedColumns("k", 2)
             .sort(field("v.2")).checkOrderedColumns("k", 2, 0, 1);
    }

    @Test
    public void testSearchGeoPointTuple() {
        TupleType tuple = TupleType.of(ProtocolVersion.NEWEST_SUPPORTED,
                                       CodecRegistry.DEFAULT_INSTANCE,
                                       cfloat(),
                                       cfloat());
        CassandraUtils.builder("tuple_indexing_geo_point")
                      .withColumn("k", "int")
                      .withColumn("v", "tuple<float, float>")
                      .withPartitionKey("k")
                      .withMapper("geo_point", geoPointMapper("v.0", "v.1"))
                      .build()
                      .createKeyspace()
                      .createTable()
                      .createIndex()
                      .insert(new String[]{"k", "v"}, new Object[]{0, tuple.newValue(40.442163f, -3.784519f)})
                      .insert(new String[]{"k", "v"}, new Object[]{1, tuple.newValue(40.575909f, -3.616095f)})
                      .insert(new String[]{"k", "v"}, new Object[]{2, tuple.newValue(38.947994f, -3.800156f)})
                      .insert(new String[]{"k", "v"}, new Object[]{3, tuple.newValue(42.546975f, 2.141841f)})
                      .insert(new String[]{"k", "v"}, new Object[]{4, tuple.newValue(49.791995f, 11.208648f)})
                      .insert(new String[]{"k", "v"}, new Object[]{5, tuple.newValue(55.337231f, 61.578869f)})
                      .insert(new String[]{"k", "v"}, new Object[]{6, tuple.newValue(41.453383f, 126.442151f)})
                      .refresh()
                      .filter(geoDistance("geo_point", 40.442163, -3.784519, "10000km"))
                      .sort(Builder.geoDistance("geo_point", 40.442163, -3.784519).reverse(false))
                      .checkOrderedColumns("k", 0, 1, 2, 3, 4, 5, 6)
                      .dropKeyspace();
    }
}
