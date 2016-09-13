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

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.stratio.cassandra.lucene.testsAT.BaseAT;
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

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */

@RunWith(JUnit4.class)
public class UDTIndexingAT extends BaseAT {

    private static CassandraUtils utils;
    private static Map<String, String> data1 = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("login", "'USER1'");
                put("first_name", "'Tom'");
                put("last_name", "'Smith'");
                put("address", "{ street: '1021 West 4th St. #202'," +
                               "city: 'San Francisco'," +
                               "zip: 94110 ," +
                               "bool: true," +
                               "height:5.4 ," +
                               "zips:[ 2,4,6 ]," +
                               "zips_map : " +
                               "{ 1 : '1A'," +
                               "2 : '2A'," +
                               "3 : '3A'}," +
                               "zips_set : {5,7,9}," +
                               "point : {" +
                               "latitude : 1.0," +
                               "longitude : -1.0" +
                               "}  " +
                               "}");

            }});
    private static Map<String, String> data2 = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("login", "'USER2'");
                put("first_name", "'Tom'");
                put("last_name", "'Smith'");
                put("address", "{ street: '1021 West 4th St. #202'," +
                               "city: 'San Francisco'," +
                               "zip: 94110 ," +
                               "bool: false," +
                               "height:5.4 ," +
                               "zips:[ 4,6,8 ]," +
                               "zips_map : " +
                               "{ 1 : '1B'," +
                               "2 : '2B'," +
                               "3 : '3B'}," +
                               "zips_set : {7,9,11}," +
                               "point : {" +
                               "latitude : 2.0," +
                               "longitude : -2.0" +
                               "}  " +
                               "}");

            }});
    private static Map<String, String> data3 = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("login", "'USER3'");
                put("first_name", "'Tom'");
                put("last_name", "'Smith'");
                put("address", "{  street: '1021 West 4th St. #202'," +
                               "city: 'San Francisco'," +
                               "zip: 94110 ," +
                               "bool: true," +
                               "height:5.4 ," +
                               "zips:[ 6,8,10 ]," +
                               "zips_map : " +
                               "{ 1 : '1A'," +
                               "2 : '2A'," +
                               "3 : '3A'}," +
                               "zips_set : {9,11,13}," +
                               "point : {" +
                               "latitude : 3.0," +
                               "longitude : -3.0" +
                               "}  " +
                               "}");

            }});
    private static Map<String, String> data4 = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("login", "'USER4'");
                put("first_name", "'Tom'");
                put("last_name", "'Smith'");
                put("address", "{ street: '1021 West 4th St. #202'," +
                               "city: 'Paris'," +
                               "zip: 94110 ," +
                               "bool: false," +
                               "height:5.4 ," +
                               "zips:[ 8,10,12 ]," +
                               "zips_map : " +
                               "{ 1 : '1B'," +
                               "2 : '2B'," +
                               "3 : '3B'}," +
                               "zips_set : {11,13,15}," +
                               "point : {" +
                               "latitude : 4.0," +
                               "longitude : -4.0" +
                               "}  " +
                               "}");

            }});
    private static Map<String, String> data5 = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("login", "'USER5'");
                put("first_name", "'Tom'");
                put("last_name", "'Smith'");
                put("address", "{ " +
                               "street: '1021 West 4th St. #202'," +
                               "city: 'Paris'," +
                               "zip: 94110 ," +
                               "bool: true," +
                               "height:5.4 ," +
                               "zips:[ 10,12,14]," +
                               "zips_map : " +
                               "{ 1 : '1A'," +
                               "2 : '2A'," +
                               "3 : '3A'}," +
                               "zips_set : {13,15,17}," +
                               "point : {" +
                               "latitude : 5.0," +
                               "longitude : -5.0" +
                               "}  " +
                               "}");

            }});
    private static Map<String, String> data6 = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("login", "'USER6'");
                put("first_name", "'Tom'");
                put("last_name", "'Smith'");
                put("address", "{  street: '1021 West 4th St. #202'," +
                               "city: 'Paris'," +
                               "zip: 94110 ," +
                               "bool: false," +
                               "height:5.4 ," +
                               "zips:[ 12,14,16 ]," +
                               "zips_map : " +
                               "{ 1 : '1B'," +
                               "2 : '2B'," +
                               "3 : '3B'}," +
                               "zips_set : {15,17,19}," +
                               "point : {" +
                               "latitude : 6.0," +
                               "longitude : -6.0" +
                               "}  " +
                               "}");

            }});
    private static Map<String, String> data7 = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("login", "'USER7'");
                put("first_name", "'Tom'");
                put("last_name", "'Smith'");
                put("address", "{ street: '1021 West 4th St. #202'," +
                               "city: 'Paris'," +
                               "zip: 94110 ," +
                               "bool: true," +
                               "height:5.4 ," +
                               "zips:[ 14,16,18 ]," +
                               "zips_map : " +
                               "{ 1 : '1A'," +
                               "2 : '2A'," +
                               "3 : '3A'}," +
                               "zips_set : {17,19,21}," +
                               "point : {" +
                               "latitude : 7.0," +
                               "longitude : -7.0" +
                               "}  " +
                               "}");

            }});
    private static Map<String, String> data8 = Collections.unmodifiableMap(
            new HashMap<String, String>() {{
                put("login", "'USER8'");
                put("first_name", "'Tom'");
                put("last_name", "'Smith'");
                put("address", "{ city: 'Madrid' }");

            }});

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("udt_indexing")
                              .withTable("udt_indexing")
                              .withIndexColumn("lucene")
                              .withUDT("geo_point", "latitude", "float")
                              .withUDT("geo_point", "longitude", "float")
                              .withUDT("address", "street", "text")
                              .withUDT("address", "city", "text")
                              .withUDT("address", "zip", "int")
                              .withUDT("address", "bool", "boolean")
                              .withUDT("address", "height", "float")
                              .withUDT("address", "point", "frozen<geo_point>")
                              .withUDT("address", "zips", "list<int>")
                              .withUDT("address", "zips_map", "map<int,text>")
                              .withUDT("address", "zips_set", "set<int>")
                              .withColumn("login", "text")
                              .withColumn("first_name", "text")
                              .withColumn("last_name", "text")
                              .withColumn("address", "frozen<address>")
                              .withMapper("address.zips", integerMapper())
                              .withMapper("address.zips_map", stringMapper())
                              .withMapper("address.zips_set", integerMapper())
                              .withMapper("address.bool", booleanMapper())
                              .withMapper("address.city", stringMapper())
                              .withMapper("address.point.latitude", floatMapper())
                              .withMapper("address.point.longitude", floatMapper())
                              .withPartitionKey("login")
                              .build()
                              .createKeyspace()
                              .createUDTs()
                              .createTable()
                              .createIndex().insert(data1, data2, data3, data4, data5, data6, data7).refresh();
    }

    @AfterClass
    public static void after() {
        utils.dropTable().dropKeyspace();
    }

    @Test
    public void testUDTInternal() {
        utils.filter(match("address.city", "Paris"))
             .checkUnorderedColumns("login", "USER4", "USER5", "USER6", "USER7");
        utils.filter(match("address.city", "San Francisco"))
             .checkUnorderedColumns("login", "USER1", "USER2", "USER3");
        utils.filter(match("address.bool", true))
             .checkUnorderedColumns("login", "USER1", "USER3", "USER5", "USER7");
        utils.filter(match("address.bool", false))
             .checkUnorderedColumns("login", "USER2", "USER4", "USER6");
    }

    @Test
    public void testUDTInternalThatFails() {
        utils.filter(match("address.point", "Paris"))
             .check(InvalidQueryException.class, "No mapper found for field 'address.point'");
    }

    @Test
    public void testUDTList() {
        utils.filter(match("address.zips", 10))
             .checkUnorderedColumns("login", "USER3", "USER4", "USER5");
        utils.filter(match("address.zips", 12))
             .checkUnorderedColumns("login", "USER4", "USER5", "USER6");
        utils.filter(match("address.zips", 14))
             .checkUnorderedColumns("login", "USER5", "USER6", "USER7");
        utils.filter(match("address.zips", 15)).check(0);
        utils.filter(match("address.zips", 16))
             .checkUnorderedColumns("login", "USER6", "USER7");
        utils.filter(match("address.zips", 18)).checkUnorderedColumns("login", "USER7");
    }

    @Test
    public void testUDTMap() {
        utils.filter(match("address.zips_map$1", "1A")).refresh(true)
             .checkUnorderedColumns("login",
                                    "USER1",
                                    "USER3",
                                    "USER5",
                                    "USER7");
        utils.filter(match("address.zips_map$1", "1B"))
             .checkUnorderedColumns("login", "USER2", "USER4", "USER6");
        utils.filter(match("address.zips_map$2", "2A"))
             .checkUnorderedColumns("login", "USER1", "USER3", "USER5", "USER7");
        utils.filter(match("address.zips_map$2", "2B"))
             .checkUnorderedColumns("login", "USER2", "USER4", "USER6");
        utils.filter(match("address.zips_map$3", "3A"))
             .checkUnorderedColumns("login", "USER1", "USER3", "USER5", "USER7");
        utils.filter(match("address.zips_map$3", "3B"))
             .checkUnorderedColumns("login", "USER2", "USER4", "USER6");
    }

    @Test
    public void testUDTMapThatFails() {
        utils.filter(match("address.zips_map", 1)).check(0);
    }

    @Test
    public void testUDTSet() {
        utils.filter(match("address.zips_set", 5)).checkUnorderedColumns("login", "USER1");
        utils.filter(match("address.zips_set", 7))
             .checkUnorderedColumns("login", "USER1", "USER2");
        utils.filter(match("address.zips_set", 9))
             .checkUnorderedColumns("login", "USER1", "USER2", "USER3");
        utils.filter(match("address.zips_set", 11))
             .checkUnorderedColumns("login", "USER2", "USER3", "USER4");
        utils.filter(match("address.zips_set", 12)).check(0);
        utils.filter(match("address.zips_set", 13))
             .checkUnorderedColumns("login", "USER3", "USER4", "USER5");
        utils.filter(match("address.zips_set", 14)).check(0);
        utils.filter(match("address.zips_set", 15))
             .checkUnorderedColumns("login", "USER4", "USER5", "USER6");
        utils.filter(match("address.zips_set", 17))
             .checkUnorderedColumns("login", "USER5", "USER6", "USER7");
        utils.filter(match("address.zips_set", 19))
             .checkUnorderedColumns("login", "USER6", "USER7");
        utils.filter(match("address.zips_set", 20)).check(0);
        utils.filter(match("address.zips_set", 21)).checkUnorderedColumns("login", "USER7");
    }

    @Test
    public void testUDTOverUDT() {
        utils.filter(match("address.point.latitude", 1.0))
             .checkUnorderedColumns("login", "USER1");
        utils.filter(match("address.point.latitude", 2.0))
             .checkUnorderedColumns("login", "USER2");
        utils.filter(match("address.point.latitude", 3.0))
             .checkUnorderedColumns("login", "USER3");
        utils.filter(match("address.point.latitude", 4.0))
             .checkUnorderedColumns("login", "USER4");
        utils.filter(match("address.point.latitude", 5.0))
             .checkUnorderedColumns("login", "USER5");
        utils.filter(match("address.point.latitude", 6.0))
             .checkUnorderedColumns("login", "USER6");
        utils.filter(match("address.point.latitude", 7.0))
             .checkUnorderedColumns("login", "USER7");
        utils.filter(match("address.point.longitude", -1.0))
             .checkUnorderedColumns("login", "USER1");
        utils.filter(match("address.point.longitude", -2.0))
             .checkUnorderedColumns("login", "USER2");
        utils.filter(match("address.point.longitude", -3.0))
             .checkUnorderedColumns("login", "USER3");
        utils.filter(match("address.point.longitude", -4.0))
             .checkUnorderedColumns("login", "USER4");
        utils.filter(match("address.point.longitude", -5.0))
             .checkUnorderedColumns("login", "USER5");
        utils.filter(match("address.point.longitude", -6.0))
             .checkUnorderedColumns("login", "USER6");
        utils.filter(match("address.point.longitude", -7.0))
             .checkUnorderedColumns("login", "USER7");
        utils.filter(range("address.point.latitude").lower(1.0)
                                                    .upper(3.0)
                                                    .includeLower(true)
                                                    .includeUpper(true))
             .checkUnorderedColumns("login", "USER1", "USER2", "USER3");
        utils.filter(range("address.point.latitude").lower(2.0)
                                                    .upper(5.0)
                                                    .includeLower(true)
                                                    .includeUpper(true))
             .checkUnorderedColumns("login", "USER2", "USER3", "USER4", "USER5");
        utils.filter(range("address.point.latitude").lower(1.0)
                                                    .upper(7.0)
                                                    .includeLower(true)
                                                    .includeUpper(true))
             .checkUnorderedColumns("login",
                                    "USER1",
                                    "USER2",
                                    "USER3",
                                    "USER4",
                                    "USER5",
                                    "USER6",
                                    "USER7");
        utils.filter(range("address.point.longitude").lower(-3.0).upper(-1.0))
             .checkUnorderedColumns("login", "USER2");
        utils.filter(range("address.point.longitude").lower(-5.0).upper(-2.0))
             .checkUnorderedColumns("login", "USER3", "USER4");
        utils.filter(range("address.point.longitude").lower(-7.0).upper(-1.0))
             .checkUnorderedColumns("login", "USER2", "USER3", "USER4", "USER5", "USER6");
        utils.filter(range("address.point.latitude").lower(1.0)
                                                    .upper(3.0)
                                                    .includeLower(true)
                                                    .includeUpper(true))
             .checkUnorderedColumns("login", "USER1", "USER2", "USER3");
        utils.filter(range("address.point.latitude").lower(2.0)
                                                    .upper(5.0)
                                                    .includeLower(true)
                                                    .includeUpper(true))
             .checkUnorderedColumns("login", "USER2", "USER3", "USER4", "USER5");
        utils.filter(range("address.point.latitude").lower(1.0)
                                                    .upper(7.0)
                                                    .includeLower(true)
                                                    .includeUpper(true))
             .checkUnorderedColumns("login",
                                    "USER1",
                                    "USER2",
                                    "USER3",
                                    "USER4",
                                    "USER5",
                                    "USER6",
                                    "USER7");
        utils.filter(range("address.point.longitude").lower(-3.0).upper(-1.0))
             .checkUnorderedColumns("login", "USER2");
        utils.filter(range("address.point.longitude").lower(-5.0).upper(-2.0))
             .checkUnorderedColumns("login", "USER3", "USER4");
        utils.filter(range("address.point.longitude").lower(-7.0).upper(-1.0))
             .checkUnorderedColumns("login", "USER2", "USER3", "USER4", "USER5", "USER6");
    }

    @Test
    public void testUDTOverUDTThatFails() {
        utils.filter(range("address.point.non-existent").lower(-1.0).upper(-3.0))
             .check(InvalidQueryException.class, "No mapper found for field 'address.point.non-existent'");
    }

    @Test
    public void testNonCompleteUDT() {
        utils.insert(data8)
             .refresh()
             .filter(match("address.city", "Madrid"))
             .checkUnorderedColumns("login", "USER8");
    }
}
