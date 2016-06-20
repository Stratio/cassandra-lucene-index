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

import com.datastax.driver.core.exceptions.DriverException;
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
import static org.junit.Assert.fail;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */

@RunWith(JUnit4.class)
public class UDTIndexingFrozenAT extends BaseAT {

    private static CassandraUtils cassandraUtils;
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
        cassandraUtils = CassandraUtils.builder("udt_indexing")
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
                                       .createIndex().insert(data1, data2, data3, data4, data5, data6, data7);
    }

    @AfterClass
    public static void after() {
        cassandraUtils.dropTable().dropKeyspace();
    }

    @Test
    public void testUDTInternal() {
        cassandraUtils.filter(match("address.city", "Paris"))
                      .checkStringColumnWithoutOrder("login", "USER4", "USER5", "USER6", "USER7");
        cassandraUtils.filter(match("address.city", "San Francisco"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3");
        cassandraUtils.filter(match("address.bool", true))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER3", "USER5", "USER7");
        cassandraUtils.filter(match("address.bool", false))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER4", "USER6");
    }

    @Test(expected = DriverException.class)
    public void testUDTInternalThatFails() {
        cassandraUtils.filter(match("address.point", "Paris")).count();
        fail("Selecting a type that is no matched must return an Exception");
    }

    @Test
    public void testUDTList() {
        cassandraUtils.filter(match("address.zips", 10))
                      .checkStringColumnWithoutOrder("login", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address.zips", 12))
                      .checkStringColumnWithoutOrder("login", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address.zips", 14))
                      .checkStringColumnWithoutOrder("login", "USER5", "USER6", "USER7");
        cassandraUtils.filter(match("address.zips", 15)).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address.zips", 16)).checkStringColumnWithoutOrder("login", "USER6", "USER7");
        cassandraUtils.filter(match("address.zips", 18)).checkStringColumnWithoutOrder("login", "USER7");
    }

    @Test
    public void testUDTMap() {
        cassandraUtils.filter(match("address.zips_map$1", "1A")).refresh(true)
                      .checkStringColumnWithoutOrder("login",
                                                     "USER1",
                                                     "USER3",
                                                     "USER5",
                                                     "USER7");
        cassandraUtils.filter(match("address.zips_map$1", "1B"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER4", "USER6");
        cassandraUtils.filter(match("address.zips_map$2", "2A"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER3", "USER5", "USER7");
        cassandraUtils.filter(match("address.zips_map$2", "2B"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER4", "USER6");
        cassandraUtils.filter(match("address.zips_map$3", "3A"))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER3", "USER5", "USER7");
        cassandraUtils.filter(match("address.zips_map$3", "3B"))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER4", "USER6");
    }

    @Test
    public void testUDTMapThatFails() {
        cassandraUtils.filter(match("address.zips_map", 1)).checkStringColumnWithoutOrder("login");
    }

    @Test
    public void testUDTSet() {
        cassandraUtils.filter(match("address.zips_set", 5)).checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("address.zips_set", 7))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2");
        cassandraUtils.filter(match("address.zips_set", 9))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3");
        cassandraUtils.filter(match("address.zips_set", 11))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER3", "USER4");
        cassandraUtils.filter(match("address.zips_set", 12)).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address.zips_set", 13))
                      .checkStringColumnWithoutOrder("login", "USER3", "USER4", "USER5");
        cassandraUtils.filter(match("address.zips_set", 14)).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address.zips_set", 15))
                      .checkStringColumnWithoutOrder("login", "USER4", "USER5", "USER6");
        cassandraUtils.filter(match("address.zips_set", 17))
                      .checkStringColumnWithoutOrder("login", "USER5", "USER6", "USER7");
        cassandraUtils.filter(match("address.zips_set", 19))
                      .checkStringColumnWithoutOrder("login", "USER6", "USER7");
        cassandraUtils.filter(match("address.zips_set", 20)).checkStringColumnWithoutOrder("login");
        cassandraUtils.filter(match("address.zips_set", 21)).checkStringColumnWithoutOrder("login", "USER7");
    }

    @Test
    public void testUDTOverUDT() {
        cassandraUtils.filter(match("address.point.latitude", 1.0)).checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("address.point.latitude", 2.0)).checkStringColumnWithoutOrder("login", "USER2");
        cassandraUtils.filter(match("address.point.latitude", 3.0)).checkStringColumnWithoutOrder("login", "USER3");
        cassandraUtils.filter(match("address.point.latitude", 4.0)).checkStringColumnWithoutOrder("login", "USER4");
        cassandraUtils.filter(match("address.point.latitude", 5.0)).checkStringColumnWithoutOrder("login", "USER5");
        cassandraUtils.filter(match("address.point.latitude", 6.0)).checkStringColumnWithoutOrder("login", "USER6");
        cassandraUtils.filter(match("address.point.latitude", 7.0)).checkStringColumnWithoutOrder("login", "USER7");
        cassandraUtils.filter(match("address.point.longitude", -1.0))
                      .checkStringColumnWithoutOrder("login", "USER1");
        cassandraUtils.filter(match("address.point.longitude", -2.0))
                      .checkStringColumnWithoutOrder("login", "USER2");
        cassandraUtils.filter(match("address.point.longitude", -3.0))
                      .checkStringColumnWithoutOrder("login", "USER3");
        cassandraUtils.filter(match("address.point.longitude", -4.0))
                      .checkStringColumnWithoutOrder("login", "USER4");
        cassandraUtils.filter(match("address.point.longitude", -5.0))
                      .checkStringColumnWithoutOrder("login", "USER5");
        cassandraUtils.filter(match("address.point.longitude", -6.0))
                      .checkStringColumnWithoutOrder("login", "USER6");
        cassandraUtils.filter(match("address.point.longitude", -7.0))
                      .checkStringColumnWithoutOrder("login", "USER7");
        cassandraUtils.filter(range("address.point.latitude").lower(1.0)
                                                             .upper(3.0)
                                                             .includeLower(true)
                                                             .includeUpper(true))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3");
        cassandraUtils.filter(range("address.point.latitude").lower(2.0)
                                                             .upper(5.0)
                                                             .includeLower(true)
                                                             .includeUpper(true))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(range("address.point.latitude").lower(1.0)
                                                             .upper(7.0)
                                                             .includeLower(true)
                                                             .includeUpper(true))
                      .checkStringColumnWithoutOrder("login",
                                                     "USER1",
                                                     "USER2",
                                                     "USER3",
                                                     "USER4",
                                                     "USER5",
                                                     "USER6",
                                                     "USER7");
        cassandraUtils.filter(range("address.point.longitude").lower(-3.0).upper(-1.0))
                      .checkStringColumnWithoutOrder("login", "USER2");
        cassandraUtils.filter(range("address.point.longitude").lower(-5.0).upper(-2.0))
                      .checkStringColumnWithoutOrder("login", "USER3", "USER4");
        cassandraUtils.filter(range("address.point.longitude").lower(-7.0).upper(-1.0))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER3", "USER4", "USER5", "USER6");
        cassandraUtils.filter(range("address.point.latitude").lower(1.0)
                                                             .upper(3.0)
                                                             .includeLower(true)
                                                             .includeUpper(true))
                      .checkStringColumnWithoutOrder("login", "USER1", "USER2", "USER3");
        cassandraUtils.filter(range("address.point.latitude").lower(2.0)
                                                             .upper(5.0)
                                                             .includeLower(true)
                                                             .includeUpper(true))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER3", "USER4", "USER5");
        cassandraUtils.filter(range("address.point.latitude").lower(1.0)
                                                             .upper(7.0)
                                                             .includeLower(true)
                                                             .includeUpper(true))
                      .checkStringColumnWithoutOrder("login",
                                                     "USER1",
                                                     "USER2",
                                                     "USER3",
                                                     "USER4",
                                                     "USER5",
                                                     "USER6",
                                                     "USER7");
        cassandraUtils.filter(range("address.point.longitude").lower(-3.0).upper(-1.0))
                      .checkStringColumnWithoutOrder("login", "USER2");
        cassandraUtils.filter(range("address.point.longitude").lower(-5.0).upper(-2.0))
                      .checkStringColumnWithoutOrder("login", "USER3", "USER4");
        cassandraUtils.filter(range("address.point.longitude").lower(-7.0).upper(-1.0))
                      .checkStringColumnWithoutOrder("login", "USER2", "USER3", "USER4", "USER5", "USER6");
    }

    @Test(expected = DriverException.class)
    public void testUDTOverUDTThatFails() {
        cassandraUtils.filter(range("address.point.non-existent").lower(-1.0).upper(-3.0)).get();
        fail("Selecting a non-existent type inside udt inside udt must return an Exception");
    }

    @Test
    public void testNonCompleteUDT() {
        cassandraUtils.insert(data8)
                      .refresh()
                      .filter(match("address.city", "Madrid"))
                      .checkStringColumnWithoutOrder("login", "USER8");
    }
}
