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

import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.*;
import static com.stratio.cassandra.lucene.testsAT.util.CassandraUtils.builder;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class UDTPartialInsertsIT extends BaseIT {

    private static CassandraUtils utils;
    private static Map<String, String> partialNull = Collections.unmodifiableMap(
            new HashMap<String, String>() {
                {
                    put("login", "'USER1'");
                    put("first_name", "'Tom'");
                    put("last_name", "'Smith'");

                }
            });

    private static Map<String, String> partialNull2 = Collections.unmodifiableMap(
            new HashMap<String, String>() {
                {
                    put("login", "'USER2'");
                    put("first_name", "'Tom'");
                    put("last_name", "'Ellis'");
                    put("address", "{ street: '1021 West 4th St. #202'," +
                                   "city: 'San Francisco'," +
                                   "zip: 94110 ," +
                                   "bool: true," +
                                   "height:5.4 }");

                }
            });
    private static Map<String, String> partialNull3 = Collections.unmodifiableMap(
            new HashMap<String, String>() {
                {
                    put("login", "'USER3'");
                    put("first_name", "'Richard'");
                    put("last_name", "'Smith'");
                    put("address", "{ street: '1021 West 4th St. #202'," +
                                   "city: 'San Francisco'," +
                                   "bool: true," +
                                   "height:5.4 ," +
                                   "point : {" +
                                   "latitude : 1.0," +
                                   "longitude : -1.0" +
                                   "}}");
                }
            });
    private static Map<String, String> partialNull4 = Collections.unmodifiableMap(
            new HashMap<String, String>() {
                {
                    put("login", "'USER4'");
                    put("first_name", "'Richard'");
                    put("last_name", "'Obama'");
                    put("address", "{ street: '1021 West 4th St. #202'," +
                                   "city: 'San Francisco'," +
                                   "zip: 94112 ," +
                                   "bool: false," +
                                   "height:5.4 ," +
                                   "point : {" +
                                   "longitude : -1.0" +
                                   "}  " +
                                   "}");
                }
            });

    private static Map<String, String> partialUpdateData = Collections.unmodifiableMap(
            new HashMap<String, String>() {
                {
                    put("login", "'USER5'");
                    put("first_name", "'Kelly'");
                    put("last_name", "'Brooks'");
                    put("address", "{ street: '15th Charleston avenue'," +
                                   "city: 'London'," +
                                   "zip: 12051 ," +
                                   "bool: false," +
                                   "height: 15.6 ," +
                                   "point : {" +
                                   "longitude : 5.34534857930, " +
                                   "latitude : 67.231478216421" +
                                   "}  " +
                                   "}");
                }
            });

    @BeforeClass
    public static void beforeClass() {
        utils = builder("udt_partial_inserts")
                .withTable("partial_inserts_table")
                .withUDT("geo_point_t", "latitude", "float")
                .withUDT("geo_point_t", "longitude", "float")
                .withUDT("address_t", "street", "text")
                .withUDT("address_t", "city", "text")
                .withUDT("address_t", "zip", "int")
                .withUDT("address_t", "bool", "boolean")
                .withUDT("address_t", "height", "float")
                .withUDT("address_t", "point", "frozen<geo_point_t>")
                .withColumn("login", "text")
                .withColumn("first_name", "text", stringMapper())
                .withColumn("last_name", "text", stringMapper())
                .withColumn("address", "frozen<address_t>")
                .withMapper("address.street", stringMapper())
                .withMapper("address.city", stringMapper())
                .withMapper("address.zip", integerMapper())
                .withMapper("address.bool", booleanMapper())
                .withMapper("place", geoPointMapper("address.point.latitude", "address.point.longitude"))
                .withMapper("address.point.latitude", floatMapper())
                .withMapper("address.point.longitude", floatMapper())
                .withIndexColumn("lucene")
                .withPartitionKey("login")
                .build()
                .createKeyspace()
                .createUDTs()
                .createTable()
                .createIndex();
    }

    @AfterClass
    public static void afterClass() {
        utils.dropTable().dropKeyspace();
    }

    @Test
    public void testPartialInserts() {
        utils.filter(all())
             .check(0)
             .insert(partialNull)
             .refresh()
             .filter(wildcard("address.street", "*"))
             .check(0)
             .filter(wildcard("address.city", "*"))
             .check(0)
             .filter(range("address.zip").lower(Integer.MIN_VALUE).upper(Integer.MAX_VALUE))
             .check(0)
             .filter(match("address.bool", true))
             .check(0)
             .filter(match("address.bool", false))
             .check(0)
             .filter(range("address.point.latitude").lower(-1 * Float.MAX_VALUE).upper(Float.MAX_VALUE))
             .check(0)
             .filter(range("address.point.longitude").lower(-1 * Float.MAX_VALUE).upper(Float.MAX_VALUE))
             .check(0)
             .filter(match("first_name", "Tom"))
             .checkUnorderedColumns("login", "USER1")
             .filter(match("first_name", "Richard"))
             .check(0)
             .filter(match("last_name", "Smith"))
             .checkUnorderedColumns("login", "USER1")
             .filter(match("last_name", "Ellis"))
             .check(0)
             .filter(match("last_name", "Obama"))
             .check(0)
             .filter(all())
             .check(1)
             .insert(partialNull2)
             .refresh()
             .filter(wildcard("address.street", "*"))
             .checkUnorderedColumns("login", "USER2")
             .filter(wildcard("address.city", "*"))
             .checkUnorderedColumns("login", "USER2")
             .filter(range("address.zip").lower(Integer.MIN_VALUE).upper(Integer.MAX_VALUE))
             .checkUnorderedColumns("login", "USER2")
             .filter(match("address.zip", 94110))
             .checkUnorderedColumns("login", "USER2")
             .filter(match("address.bool", true))
             .checkUnorderedColumns("login", "USER2")
             .filter(match("address.bool", false))
             .check(0)
             .filter(range("address.point.latitude").lower(-1 * Float.MAX_VALUE).upper(Float.MAX_VALUE))
             .check(0)
             .filter(range("address.point.longitude").lower(-1 * Float.MAX_VALUE).upper(Float.MAX_VALUE))
             .check(0)
             .filter(match("first_name", "Tom"))
             .checkUnorderedColumns("login", "USER1", "USER2")
             .filter(match("first_name", "Richard"))
             .check(0)
             .filter(match("last_name", "Smith"))
             .checkUnorderedColumns("login", "USER1")
             .filter(match("last_name", "Ellis"))
             .checkUnorderedColumns("login", "USER2")
             .filter(match("last_name", "Obama"))
             .check(0)
             .filter(all())
             .check(2)
             .insert(partialNull3)
             .refresh()
             .filter(wildcard("address.street", "*"))
             .checkUnorderedColumns("login", "USER2", "USER3")
             .filter(wildcard("address.city", "*"))
             .checkUnorderedColumns("login", "USER2", "USER3")
             .filter(range("address.zip").lower(Integer.MIN_VALUE).upper(Integer.MAX_VALUE))
             .checkUnorderedColumns("login", "USER2")
             .filter(match("address.zip", 94110))
             .checkUnorderedColumns("login", "USER2")
             .filter(match("address.bool", true))
             .checkUnorderedColumns("login", "USER2", "USER3")
             .filter(match("address.bool", false))
             .check(0)
             .filter(range("address.point.latitude").lower(-1 * Float.MAX_VALUE).upper(Float.MAX_VALUE))
             .checkUnorderedColumns("login", "USER3")
             .filter(range("address.point.longitude").lower(-1 * Float.MAX_VALUE).upper(Float.MAX_VALUE))
             .checkUnorderedColumns("login", "USER3")
             .filter(match("first_name", "Tom"))
             .checkUnorderedColumns("login", "USER1", "USER2")
             .filter(match("first_name", "Richard"))
             .checkUnorderedColumns("login", "USER3")
             .filter(match("last_name", "Smith"))
             .checkUnorderedColumns("login", "USER1", "USER3")
             .filter(match("last_name", "Ellis"))
             .checkUnorderedColumns("login", "USER2")
             .filter(match("last_name", "Obama"))
             .check(0)
             .filter(all())
             .check(3)
             .insert(partialNull4)
             .refresh()
             .filter(wildcard("address.street", "*"))
             .checkUnorderedColumns("login", "USER2", "USER3", "USER4")
             .filter(wildcard("address.city", "*"))
             .checkUnorderedColumns("login", "USER2", "USER3", "USER4")
             .filter(range("address.zip").lower(Integer.MIN_VALUE).upper(Integer.MAX_VALUE))
             .checkUnorderedColumns("login", "USER2", "USER4")
             .filter(match("address.zip", 94110))
             .checkUnorderedColumns("login", "USER2")
             .filter(match("address.zip", 94112))
             .checkUnorderedColumns("login", "USER4")
             .filter(match("address.bool", true))
             .checkUnorderedColumns("login", "USER2", "USER3")
             .filter(match("address.bool", false))
             .checkUnorderedColumns("login", "USER4")
             .filter(range("address.point.latitude").lower(-1 * Float.MAX_VALUE).upper(Float.MAX_VALUE))
             .checkUnorderedColumns("login", "USER3")
             .filter(range("address.point.longitude").lower(-1 * Float.MAX_VALUE).upper(Float.MAX_VALUE))
             .checkUnorderedColumns("login", "USER3", "USER4")
             .filter(match("first_name", "Tom"))
             .checkUnorderedColumns("login", "USER1", "USER2")
             .filter(match("first_name", "Richard"))
             .checkUnorderedColumns("login", "USER3", "USER4")
             .filter(match("last_name", "Smith"))
             .checkUnorderedColumns("login", "USER1", "USER3")
             .filter(match("last_name", "Ellis"))
             .checkUnorderedColumns("login", "USER2")
             .filter(match("last_name", "Obama"))
             .checkUnorderedColumns("login", "USER4")
             .filter(all())
             .check(4)
             .truncateTable();
    }

    @Test
    public void testPartialUpdates() {
        utils.filter(all())
             .check(0)
             .insert(partialUpdateData)
             .refresh()
             .filter(match("address.city", "London")).checkUnorderedColumns("login", "USER5")
             .filter(match("address.zip", 12051)).checkUnorderedColumns("login", "USER5")
             .filter(match("address.bool", true)).check(0)
             .filter(match("address.bool", false)).checkUnorderedColumns("login", "USER5")
             .filter(range("address.point.latitude").lower(-1 * Float.MAX_VALUE).upper(Float.MAX_VALUE))
             .check(1)
             .filter(range("address.point.longitude").lower(-1 * Float.MAX_VALUE).upper(Float.MAX_VALUE))
             .check(1)
             .filter(all())
             .check(1);

        String updateStatement = "UPDATE " +
                                 utils.getQualifiedTable() +
                                 " SET address={point:null} WHERE login='USER5'";
        utils.execute(updateStatement);
        utils.refresh()
             .filter(match("address.city", "London")).check(0)
             .filter(match("address.zip", 12051)).check(0)
             .filter(match("address.bool", true)).check(0)
             .filter(match("address.bool", false)).check(0)
             .filter(range("address.point.latitude").lower(-1 * Float.MAX_VALUE).upper(Float.MAX_VALUE))
             .check(0)
             .filter(range("address.point.longitude").lower(-1 * Float.MAX_VALUE).upper(Float.MAX_VALUE))
             .check(0)
             .filter(all())
             .check(1)
             .update().where("login", "USER5").set("address", null)
             .refresh()
             .filter(match("address.city", "London")).check(0)
             .filter(match("address.zip", 12051)).check(0)
             .filter(match("address.bool", true)).check(0)
             .filter(match("address.bool", false)).check(0)
             .filter(range("address.point.latitude").lower(-1 * Float.MAX_VALUE).upper(Float.MAX_VALUE))
             .check(0)
             .filter(range("address.point.longitude").lower(-1 * Float.MAX_VALUE).upper(Float.MAX_VALUE))
             .check(0)
             .filter(all())
             .check(1);
    }
}
