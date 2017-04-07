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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static java.util.Collections.singletonMap;

/**
 * Tests indexing of CQL maps keys, values and entries.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@RunWith(JUnit4.class)
public class MapComponentsIT extends BaseIT {

    @Test
    public void testBasicMap() {
        CassandraUtils.builder("basic_map")
                      .withPartitionKey("pk")
                      .withClusteringKey("ck")
                      .withColumn("pk", "int", null)
                      .withColumn("ck", "int", null)
                      .withColumn("map", "map<int, boolean>", null)
                      .withMapper("map", booleanMapper().column("map"))
                      .withMapper("map_key", integerMapper().column("map._key"))
                      .withMapper("map_value", booleanMapper().column("map._value"))
                      .build()
                      .createKeyspace()
                      .createTable()
                      .createIndex()
                      .insert(new String[]{"pk", "ck", "map"}, new Object[]{0, 0, singletonMap(4, true)})
                      .insert(new String[]{"pk", "ck", "map"}, new Object[]{0, 1, singletonMap(3, true)})
                      .insert(new String[]{"pk", "ck", "map"}, new Object[]{0, 2, singletonMap(2, false)})
                      .insert(new String[]{"pk", "ck", "map"}, new Object[]{0, 3, singletonMap(1, false)})
                      .insert(new String[]{"pk", "ck", "map"}, new Object[]{0, 4, singletonMap(0, false)})
                      .refresh()
                      .filter(match("map$4", true)).checkOrderedColumns("ck", 0)
                      .filter(match("map$3", true)).checkOrderedColumns("ck", 1)
                      .filter(match("map$2", false)).checkOrderedColumns("ck", 2)
                      .filter(match("map$1", false)).checkOrderedColumns("ck", 3)
                      .filter(match("map$0", false)).checkOrderedColumns("ck", 4)
                      .filter(match("map_key", 0)).checkOrderedColumns("ck", 4)
                      .filter(match("map_key", 1)).checkOrderedColumns("ck", 3)
                      .filter(match("map_key", 2)).checkOrderedColumns("ck", 2)
                      .filter(match("map_key", 3)).checkOrderedColumns("ck", 1)
                      .filter(match("map_key", 4)).checkOrderedColumns("ck", 0)
                      .filter(range("map_key").lower(0)).checkOrderedColumns("ck", 0, 1, 2, 3)
                      .filter(range("map_key").lower(1)).checkOrderedColumns("ck", 0, 1, 2)
                      .filter(range("map_key").lower(2)).checkOrderedColumns("ck", 0, 1)
                      .filter(range("map_key").lower(3)).checkOrderedColumns("ck", 0)
                      .filter(match("map_value", true)).checkOrderedColumns("ck", 0, 1)
                      .filter(match("map_value", false)).checkOrderedColumns("ck", 2, 3, 4)
                      .filter(range("map_value").upper(true)).checkOrderedColumns("ck", 2, 3, 4)
                      .filter(range("map_value").lower(false)).checkOrderedColumns("ck", 0, 1)
                      .dropKeyspace();
    }

    @Test
    public void testUDTMap() {
        CassandraUtils utils = CassandraUtils.builder("udt_map")
                                             .withUDT("udt", "u0", "text")
                                             .withUDT("udt", "u1", "int")
                                             .withPartitionKey("pk")
                                             .withClusteringKey("ck")
                                             .withColumn("pk", "int", null)
                                             .withColumn("ck", "int", null)
                                             .withColumn("map", "map<int, frozen<udt>>", null)
                                             .withMapper("map.u0", stringMapper())
                                             .withMapper("map.u1", integerMapper())
                                             .withMapper("map._key", integerMapper())
                                             .withMapper("map._value.u0", stringMapper())
                                             .withMapper("map._value.u1", integerMapper())
                                             .build()
                                             .createKeyspace()
                                             .createUDTs()
                                             .createTable()
                                             .createIndex();

        utils.execute("INSERT INTO %s (pk, ck, map) VALUES(0, 0, {5: {u0: 'a', u1: 4}})", utils.getQualifiedTable());
        utils.execute("INSERT INTO %s (pk, ck, map) VALUES(0, 1, {6: {u0: 'b', u1: 3}})", utils.getQualifiedTable());
        utils.execute("INSERT INTO %s (pk, ck, map) VALUES(0, 2, {7: {u0: 'c', u1: 2}})", utils.getQualifiedTable());
        utils.execute("INSERT INTO %s (pk, ck, map) VALUES(0, 3, {8: {u0: 'd', u1: 1}})", utils.getQualifiedTable());
        utils.execute("INSERT INTO %s (pk, ck, map) VALUES(0, 4, {9: {u0: 'e', u1: 0}})", utils.getQualifiedTable());
        utils.refresh();

        utils.filter(match("map.u0$5", "a")).checkOrderedColumns("ck", 0)
             .filter(match("map.u0$6", "b")).checkOrderedColumns("ck", 1)
             .filter(match("map.u0$7", "c")).checkOrderedColumns("ck", 2)
             .filter(match("map.u0$8", "d")).checkOrderedColumns("ck", 3)
             .filter(match("map.u0$9", "e")).checkOrderedColumns("ck", 4)
             .filter(range("map.u0$5").upper("d")).checkOrderedColumns("ck", 0);

        utils.filter(match("map.u1$5", 4)).checkOrderedColumns("ck", 0)
             .filter(match("map.u1$6", 3)).checkOrderedColumns("ck", 1)
             .filter(match("map.u1$7", 2)).checkOrderedColumns("ck", 2)
             .filter(match("map.u1$8", 1)).checkOrderedColumns("ck", 3)
             .filter(match("map.u1$9", 0)).checkOrderedColumns("ck", 4)
             .filter(range("map.u1$5").lower(1)).checkOrderedColumns("ck", 0);

        utils.filter(match("map._key", 5)).checkOrderedColumns("ck", 0)
             .filter(match("map._key", 6)).checkOrderedColumns("ck", 1)
             .filter(match("map._key", 7)).checkOrderedColumns("ck", 2)
             .filter(match("map._key", 8)).checkOrderedColumns("ck", 3)
             .filter(match("map._key", 9)).checkOrderedColumns("ck", 4)
             .filter(range("map._key").lower(4)).checkOrderedColumns("ck", 0, 1, 2, 3, 4)
             .filter(range("map._key").lower(5)).checkOrderedColumns("ck", 1, 2, 3, 4)
             .filter(range("map._key").lower(6)).checkOrderedColumns("ck", 2, 3, 4)
             .filter(range("map._key").lower(7)).checkOrderedColumns("ck", 3, 4)
             .filter(range("map._key").lower(8)).checkOrderedColumns("ck", 4);

        utils.filter(match("map._value.u0", "a")).checkOrderedColumns("ck", 0)
             .filter(match("map._value.u0", "b")).checkOrderedColumns("ck", 1)
             .filter(match("map._value.u0", "c")).checkOrderedColumns("ck", 2)
             .filter(match("map._value.u0", "d")).checkOrderedColumns("ck", 3)
             .filter(match("map._value.u0", "e")).checkOrderedColumns("ck", 4)
             .filter(range("map._value.u0").upper("d")).checkOrderedColumns("ck", 0, 1, 2);

        utils.filter(match("map._value.u1", 4)).checkOrderedColumns("ck", 0)
             .filter(match("map._value.u1", 3)).checkOrderedColumns("ck", 1)
             .filter(match("map._value.u1", 2)).checkOrderedColumns("ck", 2)
             .filter(match("map._value.u1", 1)).checkOrderedColumns("ck", 3)
             .filter(match("map._value.u1", 0)).checkOrderedColumns("ck", 4)
             .filter(range("map._value.u1").lower(1)).checkOrderedColumns("ck", 0, 1, 2);

        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }
}
