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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.*;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
@RunWith(JUnit4.class)
public class GeoSearchOverIndexedGeoShapesAT extends BaseAT {

    public static final Map<String, String> data1, data2, data3, data4, data5, data6, data7, data8, data9, data10,
            data11, data12;
    protected static CassandraUtils cassandraUtils;

    static {
        data1 = new LinkedHashMap<>();
        data1.put("place", "'SHAPE_1'");
        data1.put("shape",
                  "'POLYGON((-3.798180 40.444563,-3.789082 40.442473,-3.796077 40.437835," +
                  "-3.793201 40.441427,-3.798180 40.444563))'");

        data2 = new LinkedHashMap<>();
        data2.put("place", "'SHAPE_2'");
        data2.put("shape",
                  "'POLYGON((-3.8012266 40.4384634, -3.7821293000000002 40.44178680000001," +
                  "-3.7937164 40.4453468, -3.7937164 40.453054, -3.8012266 40.4384634))'");

        data3 = new LinkedHashMap<>();
        data3.put("place", "'SHAPE_3'");
        data3.put("shape", "'POLYGON((-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001," +
                           "-3.785691299999999 40.445020199999995,-3.781742999999999 40.43427419999999," +
                           "-3.7777947999999997 40.4497883,-3.8094234 40.44858,-3.8033294999999994 40.4349602))'");

        data4 = new LinkedHashMap<>();
        data4.put("place", "'LINE_1'");
        data4.put("shape", "'LINESTRING (30 10, 10 30, 40 40)'");

        data5 = new LinkedHashMap<>();
        data5.put("place", "'LINE_2'");
        data5.put("shape", "'LINEARRING(30 10, 10 30, 40 40,30 10)'");

        data6 = new LinkedHashMap<>();
        data6.put("place", "'POLYGON_1'");
        data6.put("shape", "'POLYGON((0.0 0.0,0.0 10.0,10.0 0.0,0.0 0.0))'");

        data7 = new LinkedHashMap<>();
        data7.put("place", "'POLYGON_2'");
        data7.put("shape", "'POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))'");

        data8 = new LinkedHashMap<>();
        data8.put("place", "'MULTIPOINT_1'");
        data8.put("shape", "'MULTIPOINT ((10 40), (40 30), (20 20), (30 10))'");

        data9 = new LinkedHashMap<>();
        data9.put("place", "'MULTIPOINT_2'");
        data9.put("shape", "'MULTIPOINT (10 40, 40 30, 20 20, 30 10)'");

        data10 = new LinkedHashMap<>();
        data10.put("place", "'MULTILINESTRING_1'");
        data10.put("shape", "'MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))'");

        data11 = new LinkedHashMap<>();
        data11.put("place", "'MULTIPOLYGON_1'");
        data11.put("shape", "'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))'");

        data12 = new LinkedHashMap<>();
        data12.put("place", "'MULTIPOLYGON_2'");
        data12.put("shape",
                   "'MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))'");

    }

    @BeforeClass
    public static void setUpSuite() {
        cassandraUtils = CassandraUtils.builder("search")
                                       .withPartitionKey("place")
                                       .withClusteringKey()
                                       .withColumn("lucene", "text", null)
                                       .withColumn("place", "text", null)
                                       .withColumn("shape", "text", geoShapeMapper())
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
    public void testIntersectsPoint1() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.793030 40.435450)").operation("intersects"))
                      .checkStringColumnWithoutOrder("place");

    }

    @Test
    public void testContainsPoint1() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.793030 40.435450)").operation("contains"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testIsWithinPoint1() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.793030 40.435450)").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testIntersectsPoint2() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.789382 40.436169)").operation("intersects"))
                      .checkStringColumnWithoutOrder("place");

    }

    @Test
    public void testContainsPoint2() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.789382 40.436169)").operation("contains"))
                      .checkStringColumnWithoutOrder("place");

    }

    @Test
    public void testIsWithinPoint2() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.789382 40.436169)").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");

    }

    @Test
    public void testIntersectsPoint3() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.789725 40.446751)").operation("intersects"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_3");
    }

    @Test
    public void testContainsPoint3() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.789725 40.446751)").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_3");
    }

    @Test
    public void testIsWithinPoint3() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.789725 40.446751)").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testIntersectsPoint4() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.792343 40.446522)").operation("intersects"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_3");
    }

    @Test
    public void testContainsPoint4() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.792343 40.446522)").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_3");
    }

    @Test
    public void testIsWithinPoint4() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.792343 40.446522)").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testIntersectsPoint5() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.804402 40.444040)").operation("intersects"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_3");
    }

    @Test
    public void testContainsPoint5() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.804402 40.444040)").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_3");
    }

    @Test
    public void testIsWithinPoint5() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.804402 40.444040)").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testIntersectsPoint6() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.803630 40.436724)").operation("intersects"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_3");
    }

    @Test
    public void testContainsPoint6() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.803630 40.436724)").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_3");
    }

    @Test
    public void testIsWithinPoint6() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.803630 40.436724)").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testIntersectsPoint7() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.792472 40.440938)").operation("intersects"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testContainsPoint7() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.792472 40.440938)").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint7() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.792472 40.440938)").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testIntersectsPoint8() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.790541 40.442113)").operation("intersects"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testContainsPoint8() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.790541 40.442113)").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint8() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.790541 40.442113)").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testIntersectsPoint9() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.794575 40.443159)").operation("intersects"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testContainsPoint9() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.794575 40.443159)").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint9() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.794575 40.443159)").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testIntersectsPoint10() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.795648 40.441264)").operation("intersects"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_2");
    }

    @Test
    public void testContainsPoint10() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.795648 40.441264)").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint10() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.795648 40.441264)").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testIntersectsPoint11() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.796248 40.442342)").operation("intersects"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_2");
    }

    @Test
    public void testContainsPoint11() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.796248 40.442342)").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint11() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.796248 40.442342)").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testIntersectsPoint12() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.795862 40.440676)").operation("intersects"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_2");
    }

    @Test
    public void testContainsPoint12() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.795862 40.440676)").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint12() {
        cassandraUtils.filter(geoShape("shape", "POINT(-3.795862 40.440676)").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testStarShapedIntersectsQuery() {
        cassandraUtils.filter(geoShape("shape",
                                       "POLYGON((-3.798180 40.444563,-3.789082 40.442473,-3.796077 40.437835, " +
                                       "-3.793201 40.441427,-3.798180 40.444563))").operation("intersects"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testStarShapedIntersectsQuery2() {
        cassandraUtils.filter(geoShape("shape",
                                       "POLYGON((-3.8012266 40.4384634, -3.7821293000000002 40.44178680000001, " +
                                       "-3.7937164 40.4453468, -3.7937164 40.453054, -3.8012266 40.4384634))").operation(
                "intersects")).checkStringColumnWithoutOrder("place", "SHAPE_1", "SHAPE_2", "SHAPE_3");
    }

    @Test
    public void testConcaveShapesIntersectsQuery() {
        cassandraUtils.filter(geoShape("shape",
                                       "POLYGON((-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001," +
                                       "-3.785691299999999 40.445020199999995,-3.781742999999999 40.43427419999999," +
                                       "-3.7777947999999997 40.4497883,-3.8094234 40.44858,-3.8033294999999994 40.4349602))")
                                      .operation("intersects"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_2", "SHAPE_3");

    }

    @Test
    public void testStarShapedContainsQuery() {
        cassandraUtils.filter(geoShape("shape",
                                       "POLYGON((-3.798180 40.444563,-3.789082 40.442473,-3.796077 40.437835, " +
                                       "-3.793201 40.441427,-3.798180 40.444563))").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "SHAPE_1");
    }

    @Test
    public void testStarShapedContainsQuery2() {
        cassandraUtils.filter(geoShape("shape",
                                       "POLYGON((-3.8012266 40.4384634, -3.7821293000000002 40.44178680000001, " +
                                       "-3.7937164 40.4453468, -3.7937164 40.453054, -3.8012266 40.4384634))").operation(
                "contains")).checkStringColumnWithoutOrder("place", "SHAPE_2");
    }

    @Test
    public void testConcaveShapesContainsQuery() {
        cassandraUtils.filter(geoShape("shape",
                                       "POLYGON((-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001," +
                                       "-3.785691299999999 40.445020199999995,-3.781742999999999 40.43427419999999," +
                                       "-3.7777947999999997 40.4497883,-3.8094234 40.44858,-3.8033294999999994 40.4349602))")
                                      .operation("contains")).checkStringColumnWithoutOrder("place", "SHAPE_3");

    }

    @Test
    public void testStarShapedIsWithinQuery() {
        cassandraUtils.filter(geoShape("shape",
                                       "POLYGON((-3.798180 40.444563,-3.789082 40.442473,-3.796077 40.437835, " +
                                       "-3.793201 40.441427,-3.798180 40.444563))").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testStarShapedIsWithinQuery2() {
        cassandraUtils.filter(geoShape("shape",
                                       "POLYGON((-3.8012266 40.4384634, -3.7821293000000002 40.44178680000001, " +
                                       "-3.7937164 40.4453468, -3.7937164 40.453054, -3.8012266 40.4384634))").operation(
                "is_within")).checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testConcaveShapesIsWithinQuery() {
        cassandraUtils.filter(geoShape("shape",
                                       "POLYGON((-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001," +
                                       "-3.785691299999999 40.445020199999995,-3.781742999999999 40.43427419999999," +
                                       "-3.7777947999999997 40.4497883,-3.8094234 40.44858,-3.8033294999999994 40.4349602))")
                                      .operation("is_within")).checkStringColumnWithoutOrder("place");

    }

    @Test
    public void testBufferIntersectsShape() {
        cassandraUtils.filter(geoShape("shape",
                                       "LINESTRING(-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001," +
                                       "-3.785691299999999 40.445020199999995)").operation("intersects")
                                                                                .transform(bufferGeoTransformation().maxDistance(
                                                                                        "500m")))
                      .checkStringColumnWithoutOrder("place", "SHAPE_1", "SHAPE_2", "SHAPE_3");

    }

    @Test
    public void testBufferContainsShape() {
        cassandraUtils.filter(geoShape("shape",
                                       "LINESTRING(-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001," +
                                       "-3.785691299999999 40.445020199999995)").operation("contains")
                                                                                .transform(bufferGeoTransformation().maxDistance(
                                                                                        "500m")))
                      .checkStringColumnWithoutOrder("place");

    }

    @Test
    public void testBufferIsWithinShape() {
        cassandraUtils.filter(geoShape("shape",
                                       "LINESTRING(-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001," +
                                       "-3.785691299999999 40.445020199999995)").operation("is_within")
                                                                                .transform(bufferGeoTransformation().maxDistance(
                                                                                        "500m")))
                      .checkStringColumnWithoutOrder("place");

    }

    @Test
    public void testLine1Intersects() {
        cassandraUtils.filter(geoShape("shape", "LINESTRING (30 10, 10 30, 40 40)").operation("intersects"))
                      .checkStringColumnWithoutOrder("place",
                                                     "LINE_1",
                                                     "LINE_2",
                                                     "MULTILINESTRING_1",
                                                     "MULTIPOINT_1",
                                                     "MULTIPOINT_2",
                                                     "MULTIPOLYGON_1",
                                                     "MULTIPOLYGON_2",
                                                     "POLYGON_2");
    }

    @Test
    public void testLine1Contains() {
        cassandraUtils.filter(geoShape("shape", "LINESTRING (30 10, 10 30, 40 40)").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "LINE_1", "LINE_2");
    }

    @Test
    public void testLine1IsWithin() {
        cassandraUtils.filter(geoShape("shape", "LINESTRING (30 10, 10 30, 40 40)").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testLine2Intersects() {
        cassandraUtils.filter(geoShape("shape", "LINEARRING(30 10, 10 30, 40 40,30 10)").operation("intersects"))
                      .checkStringColumnWithoutOrder("place",
                                                     "LINE_1",
                                                     "LINE_2",
                                                     "MULTILINESTRING_1",
                                                     "MULTIPOINT_1",
                                                     "MULTIPOINT_2",
                                                     "MULTIPOLYGON_1",
                                                     "MULTIPOLYGON_2",
                                                     "POLYGON_2");
    }

    @Test
    public void testLine2Contains() {
        cassandraUtils.filter(geoShape("shape", "LINEARRING(30 10, 10 30, 40 40,30 10)").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "LINE_2");
    }

    @Test
    public void testLine2IsWithin() {
        cassandraUtils.filter(geoShape("shape", "LINEARRING(30 10, 10 30, 40 40,30 10)").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testPolygonIntersects() {
        cassandraUtils.filter(geoShape("shape", "POLYGON((0.0 0.0,0.0 10.0,10.0 0.0,0.0 0.0))").operation("intersects"))
                      .checkStringColumnWithoutOrder("place", "POLYGON_1");
    }

    @Test
    public void testPolygonContains() {
        cassandraUtils.filter(geoShape("shape", "POLYGON((0.0 0.0,0.0 10.0,10.0 0.0,0.0 0.0))").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "POLYGON_1");
    }

    @Test
    public void testPolygonIsWithin() {
        cassandraUtils.filter(geoShape("shape", "POLYGON((0.0 0.0,0.0 10.0,10.0 0.0,0.0 0.0))").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testPolygon2Intersects() {
        cassandraUtils.filter(geoShape("shape",
                                       "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))").operation(
                "intersects"))
                      .checkStringColumnWithoutOrder("place",
                                                     "LINE_1",
                                                     "LINE_2",
                                                     "MULTILINESTRING_1",
                                                     "MULTIPOINT_1",
                                                     "MULTIPOINT_2",
                                                     "MULTIPOLYGON_1",
                                                     "MULTIPOLYGON_2",
                                                     "POLYGON_2");
    }

    @Test
    public void testPolygon2Contains() {
        cassandraUtils.filter(geoShape("shape",
                                       "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))").operation(
                "contains")).checkStringColumnWithoutOrder("place", "POLYGON_2");
    }

    @Test
    public void testPolygon2IsWithin() {
        cassandraUtils.filter(geoShape("shape",
                                       "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))").operation(
                "is_within")).checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testMultipointIntersects() {
        cassandraUtils.filter(geoShape("shape", "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))").operation(
                "intersects"))
                      .checkStringColumnWithoutOrder("place",
                                                     "LINE_1",
                                                     "LINE_2",
                                                     "MULTILINESTRING_1",
                                                     "MULTIPOINT_1",
                                                     "MULTIPOINT_2",
                                                     "MULTIPOLYGON_1",
                                                     "MULTIPOLYGON_2",
                                                     "POLYGON_2");
    }

    @Test
    public void testMultipointContains() {
        cassandraUtils.filter(geoShape("shape",
                                       "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "MULTIPOINT_1", "MULTIPOINT_2");
    }

    @Test
    public void testMultipointIsWithin() {
        cassandraUtils.filter(geoShape("shape",
                                       "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testMultipoint2Intersects() {
        cassandraUtils.filter(geoShape("shape", "MULTIPOINT (10 40, 40 30, 20 20, 30 10)").operation("intersects"))
                      .checkStringColumnWithoutOrder("place",
                                                     "LINE_1",
                                                     "LINE_2",
                                                     "MULTILINESTRING_1",
                                                     "MULTIPOINT_1",
                                                     "MULTIPOINT_2",
                                                     "MULTIPOLYGON_1",
                                                     "MULTIPOLYGON_2",
                                                     "POLYGON_2");
    }

    @Test
    public void testMultipoint2Contains() {
        cassandraUtils.filter(geoShape("shape", "MULTIPOINT (10 40, 40 30, 20 20, 30 10)").operation("contains"))
                      .checkStringColumnWithoutOrder("place", "MULTIPOINT_1", "MULTIPOINT_2");
    }

    @Test
    public void testMultipoint2IsWithin() {
        cassandraUtils.filter(geoShape("shape", "MULTIPOINT (10 40, 40 30, 20 20, 30 10)").operation("is_within"))
                      .checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testMultilineIntersects() {
        cassandraUtils.filter(geoShape("shape",
                                       "MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))").operation(
                "intersects"))
                      .checkStringColumnWithoutOrder("place",
                                                     "LINE_1",
                                                     "LINE_2",
                                                     "MULTILINESTRING_1",
                                                     "MULTIPOINT_1",
                                                     "MULTIPOINT_2",
                                                     "MULTIPOLYGON_1",
                                                     "MULTIPOLYGON_2",
                                                     "POLYGON_2");
    }

    @Test
    public void testMultilineContains() {
        cassandraUtils.filter(geoShape("shape",
                                       "MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))").operation(
                "contains")).checkStringColumnWithoutOrder("place", "MULTILINESTRING_1");
    }

    @Test
    public void testMultilineIsWithin() {
        cassandraUtils.filter(geoShape("shape",
                                       "MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))").operation(
                "is_within")).checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testMultipolygonIntersects() {
        cassandraUtils.filter(geoShape("shape",
                                       "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))")
                                      .operation("intersects"))
                      .checkStringColumnWithoutOrder("place",
                                                     "LINE_1",
                                                     "LINE_2",
                                                     "MULTILINESTRING_1",
                                                     "MULTIPOINT_1",
                                                     "MULTIPOINT_2",
                                                     "MULTIPOLYGON_1",
                                                     "MULTIPOLYGON_2",
                                                     "POLYGON_2");
    }

    @Test
    public void testMultipolygonContains() {
        cassandraUtils.filter(geoShape("shape",
                                       "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))")
                                      .operation("contains")).checkStringColumnWithoutOrder("place", "MULTIPOLYGON_1");
    }

    @Test
    public void testMultipolygonIsWithin() {
        cassandraUtils.filter(geoShape("shape",
                                       "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))")
                                      .operation("is_within")).checkStringColumnWithoutOrder("place");
    }

    @Test
    public void testMultipolygon2Intersects() {
        cassandraUtils.filter(geoShape("shape",
                                       "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))")
                                      .operation("intersects"))
                      .checkStringColumnWithoutOrder("place",
                                                     "LINE_1",
                                                     "LINE_2",
                                                     "MULTILINESTRING_1",
                                                     "MULTIPOINT_1",
                                                     "MULTIPOINT_2",
                                                     "MULTIPOLYGON_1",
                                                     "MULTIPOLYGON_2",
                                                     "POLYGON_2");
    }

    @Test
    public void testMultipolygon2Contains() {
        cassandraUtils.filter(geoShape("shape",
                                       "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))")
                                      .operation("contains")).checkStringColumnWithoutOrder("place", "MULTIPOLYGON_2");
    }

    @Test
    public void testMultipolygon2IsWithin() {
        cassandraUtils.filter(geoShape("shape",
                                       "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))")
                                      .operation("is_within")).checkStringColumnWithoutOrder("place");
    }
}
