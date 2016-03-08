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

package com.stratio.cassandra.lucene.testsAT.search;

import com.stratio.cassandra.lucene.testsAT.BaseAT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.*;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoShapeSearchOverIndexedGeoPointsAT extends BaseAT {

    protected static CassandraUtils cassandraUtils;

    public static final Map<String, String> data1, data2, data3, data4, data5, data6, data7, data8, data9, data10,
            data11, data12;

    static {
        data1 = new LinkedHashMap<>();
        data1.put("place", "'POINT_1'");
        data1.put("longitude", "-3.793030");
        data1.put("latitude", "40.435450");

        data2 = new LinkedHashMap<>();
        data2.put("place", "'POINT_2'");
        data2.put("longitude", "-3.789382");
        data2.put("latitude", "40.436169");

        data3 = new LinkedHashMap<>();
        data3.put("place", "'POINT_3'");
        data3.put("longitude", "-3.789725");
        data3.put("latitude", "40.446751");

        data4 = new LinkedHashMap<>();
        data4.put("place", "'POINT_4'");
        data4.put("longitude", "-3.792343");
        data4.put("latitude", "40.446522");

        data5 = new LinkedHashMap<>();
        data5.put("place", "'POINT_5'");
        data5.put("longitude", "-3.804402");
        data5.put("latitude", "40.444040");

        data6 = new LinkedHashMap<>();
        data6.put("place", "'POINT_6'");
        data6.put("longitude", "-3.803630");
        data6.put("latitude", "40.436724");

        data7 = new LinkedHashMap<>();
        data7.put("place", "'POINT_7'");
        data7.put("longitude", "-3.792472");
        data7.put("latitude", "40.440938");

        data8 = new LinkedHashMap<>();
        data8.put("place", "'POINT_8'");
        data8.put("longitude", "-3.790541");
        data8.put("latitude", "40.442113");

        data9 = new LinkedHashMap<>();
        data9.put("place", "'POINT_9'");
        data9.put("longitude", "-3.794575");
        data9.put("latitude", "40.443159");

        data10 = new LinkedHashMap<>();
        data10.put("place", "'POINT_10'");
        data10.put("longitude", "-3.795648");
        data10.put("latitude", "40.441264");

        data11 = new LinkedHashMap<>();
        data11.put("place", "'POINT_11'");
        data11.put("longitude", "-3.796248");
        data11.put("latitude", "40.442342");

        data12 = new LinkedHashMap<>();
        data12.put("place", "'POINT_12'");
        data12.put("longitude", "-3.795862");
        data12.put("latitude", "40.440676");
    }

    @BeforeClass
    public static void setUpSuite() {
        cassandraUtils = CassandraUtils.builder("search")
                                       .withPartitionKey("place")
                                       .withClusteringKey()
                                       .withColumn("lucene", "text", null)
                                       .withColumn("place", "text", null)
                                       .withColumn("latitude", "decimal", null)
                                       .withColumn("longitude", "decimal", null)
                                       .withMapper("location", geoPointMapper("latitude", "longitude"))
                                       .build()
                                       .createKeyspace()
                                       .createTable()
                                       .createIndex()
                                       .insert(data1,
                                               data2,
                                               data3,
                                               data4,
                                               data5,
                                               data6,
                                               data7,
                                               data8,
                                               data9,
                                               data10,
                                               data11,
                                               data12)
                                       .refresh();
    }

    @AfterClass
    public static void tearDownSuite() {
        cassandraUtils.dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void testStarShapedIntersectsQuery() {
        //query star shaped shape
        cassandraUtils.filter(geoShape("location",
                                       "POLYGON((-3.798180 40.444563,-3.789082 40.442473,-3.796077 40.437835, " +
                                       "-3.793201 40.441427,-3.798180 40.444563))").operation("intersects"))
                      .checkStringColumnWithoutOrder("place", "POINT_7", "POINT_8", "POINT_9");
    }

    @Test
    public void testStarShapedIntersectsQuery2() {
        //query star shaped shape
        cassandraUtils.filter(geoShape("location",
                                       "POLYGON((-3.8012266 40.4384634, -3.7821293000000002 40.44178680000001, " +
                                       "-3.7937164 40.4453468, -3.7937164 40.453054, -3.8012266 40.4384634))").operation(
                "intersects"))
                      .checkStringColumnWithoutOrder("place",
                                                     "POINT_7",
                                                     "POINT_8",
                                                     "POINT_9",
                                                     "POINT_10",
                                                     "POINT_11",
                                                     "POINT_12");
    }

    @Test
    public void testConcaveShapesIntersectsQuery() {
        cassandraUtils.filter(geoShape("location",
                                       "POLYGON((-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001," +
                                       "-3.785691299999999 40.445020199999995,-3.781742999999999 40.43427419999999," +
                                       "-3.7777947999999997 40.4497883,-3.8094234 40.44858,-3.8033294999999994 40.4349602))")
                                      .operation("intersects"))
                      .checkStringColumnWithoutOrder("place", "POINT_3", "POINT_4", "POINT_5", "POINT_6");

    }

    @Test
    public void testStarShapedContainsQuery() {
        //query star shaped shape
        cassandraUtils.filter(geoShape("location",
                                       "POLYGON((-3.798180 40.444563,-3.789082 40.442473,-3.796077 40.437835, " +
                                       "-3.793201 40.441427,-3.798180 40.444563))").operation("contains"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testStarShapedContainsQuery2() {
        //query star shaped shape
        cassandraUtils.filter(geoShape("location",
                                       "POLYGON((-3.8012266 40.4384634, -3.7821293000000002 40.44178680000001, " +
                                       "-3.7937164 40.4453468, -3.7937164 40.453054, -3.8012266 40.4384634))").operation(
                "contains")).checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testConcaveShapesContainsQuery() {
        cassandraUtils.filter(geoShape("location",
                                       "POLYGON((-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001," +
                                       "-3.785691299999999 40.445020199999995,-3.781742999999999 40.43427419999999," +
                                       "-3.7777947999999997 40.4497883,-3.8094234 40.44858,-3.8033294999999994 40.4349602))")
                                      .operation("contains")).checkStringColumnWithoutOrder("place");

    }

    @Test
    public void testStarShapedIsWithinQuery() {
        //query star shaped shape
        cassandraUtils.filter(geoShape("location",
                                       "POLYGON((-3.798180 40.444563,-3.789082 40.442473,-3.796077 40.437835, " +
                                       "-3.793201 40.441427,-3.798180 40.444563))").operation("is_within"))
                      .checkStringColumnWithoutOrder("place", "POINT_7", "POINT_8", "POINT_9");
    }

    @Test
    public void testStarShapedIsWithinQuery2() {
        //query star shaped shape
        cassandraUtils.filter(geoShape("location",
                                       "POLYGON((-3.8012266 40.4384634, -3.7821293000000002 40.44178680000001, " +
                                       "-3.7937164 40.4453468, -3.7937164 40.453054, -3.8012266 40.4384634))").operation(
                "is_within"))
                      .checkStringColumnWithoutOrder("place",
                                                     "POINT_7",
                                                     "POINT_8",
                                                     "POINT_9",
                                                     "POINT_10",
                                                     "POINT_11",
                                                     "POINT_12");
    }

    @Test
    public void testConcaveShapesIsWithinQuery() {
        cassandraUtils.filter(geoShape("location",
                                       "POLYGON((-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001," +
                                       "-3.785691299999999 40.445020199999995,-3.781742999999999 40.43427419999999," +
                                       "-3.7777947999999997 40.4497883,-3.8094234 40.44858,-3.8033294999999994 40.4349602))")
                                      .operation("is_within"))
                      .checkStringColumnWithoutOrder("place", "POINT_3", "POINT_4", "POINT_5", "POINT_6");

    }

    @Test
    public void testBufferShape() {
        cassandraUtils.filter(geoShape("location",
                                       "LINESTRING(-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001," +
                                       "-3.785691299999999 40.445020199999995)").operation("intersects")
                                                                                .transform(bufferGeoTransformation().maxDistance(
                                                                                        "500m")))
                      .checkStringColumnWithoutOrder("place",
                                                     "POINT_3",
                                                     "POINT_4",
                                                     "POINT_6",
                                                     "POINT_7",
                                                     "POINT_8",
                                                     "POINT_9",
                                                     "POINT_10",
                                                     "POINT_11",
                                                     "POINT_12");

    }

}
