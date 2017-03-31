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
package com.stratio.cassandra.lucene.testsAT.geospatial;

import com.stratio.cassandra.lucene.builder.common.GeoShape;
import com.stratio.cassandra.lucene.testsAT.BaseIT;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtils;
import com.stratio.cassandra.lucene.testsAT.util.CassandraUtilsSelect;
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
public class GeoShapeSearchOverGeoShapesIT extends BaseIT {

    public static final Map<String, String> data1, data2, data3, data4, data5, data6, data7, data8, data9, data10,
            data11, data12;
    protected static CassandraUtils utils;

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
                   "'MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35)," +
                   "(30 20, 20 15, 20 25, 30 20)))'");

    }

    @BeforeClass
    public static void before() {
        utils = CassandraUtils.builder("search")
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
    public static void after() {
        CassandraUtils.dropKeyspaceIfNotNull(utils);
    }

    private CassandraUtilsSelect filter(String shape, String operation) {
        return utils.filter(geoShape("shape", shape).operation(operation));
    }

    private CassandraUtilsSelect filter(GeoShape shape, String operation) {
        return utils.filter(geoShape("shape", shape).operation(operation));
    }

    @Test
    public void testIntersectsPoint1() {
        filter("POINT(-3.793030 40.435450)", "intersects").check(0);
    }

    @Test
    public void testContainsPoint1() {
        filter("POINT(-3.793030 40.435450)", "contains").check(0);
    }

    @Test
    public void testIsWithinPoint1() {
        filter("POINT(-3.793030 40.435450)", "is_within").check(0);
    }

    @Test
    public void testIntersectsPoint2() {
        filter("POINT(-3.789382 40.436169)", "intersects").check(0);
    }

    @Test
    public void testContainsPoint2() {
        filter("POINT(-3.789382 40.436169)", "contains").check(0);
    }

    @Test
    public void testIsWithinPoint2() {
        filter("POINT(-3.789382 40.436169)", "is_within").check(0);
    }

    @Test
    public void testIntersectsPoint3() {
        filter("POINT(-3.789725 40.446751)", "intersects").checkUnorderedColumns("place", "SHAPE_3");
    }

    @Test
    public void testContainsPoint3() {
        filter("POINT(-3.789725 40.446751)", "contains").checkUnorderedColumns("place", "SHAPE_3");
    }

    @Test
    public void testIsWithinPoint3() {
        filter("POINT(-3.789725 40.446751)", "is_within").check(0);
    }

    @Test
    public void testIntersectsPoint4() {
        filter("POINT(-3.792343 40.446522)", "intersects").checkUnorderedColumns("place", "SHAPE_3");
    }

    @Test
    public void testContainsPoint4() {
        filter("POINT(-3.792343 40.446522)", "contains").checkUnorderedColumns("place", "SHAPE_3");
    }

    @Test
    public void testIsWithinPoint4() {
        filter("POINT(-3.792343 40.446522)", "is_within").check(0);
    }

    @Test
    public void testIntersectsPoint5() {
        filter("POINT(-3.804402 40.444040)", "intersects").checkUnorderedColumns("place", "SHAPE_3");
    }

    @Test
    public void testContainsPoint5() {
        filter("POINT(-3.804402 40.444040)", "contains").checkUnorderedColumns("place", "SHAPE_3");
    }

    @Test
    public void testIsWithinPoint5() {
        filter("POINT(-3.804402 40.444040)", "is_within").check(0);
    }

    @Test
    public void testIntersectsPoint6() {
        filter("POINT(-3.803630 40.436724)", "intersects").checkUnorderedColumns("place", "SHAPE_3");
    }

    @Test
    public void testContainsPoint6() {
        filter("POINT(-3.803630 40.436724)", "contains").checkUnorderedColumns("place", "SHAPE_3");
    }

    @Test
    public void testIsWithinPoint6() {
        filter("POINT(-3.803630 40.436724)", "is_within").check(0);
    }

    @Test
    public void testIntersectsPoint7() {
        filter("POINT(-3.792472 40.440938)", "intersects").checkUnorderedColumns("place", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testContainsPoint7() {
        filter("POINT(-3.792472 40.440938)", "contains").checkUnorderedColumns("place", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint7() {
        filter("POINT(-3.792472 40.440938)", "is_within").check(0);
    }

    @Test
    public void testIntersectsPoint8() {
        filter("POINT(-3.790541 40.442113)", "intersects").checkUnorderedColumns("place", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testContainsPoint8() {
        filter("POINT(-3.790541 40.442113)", "contains").checkUnorderedColumns("place", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint8() {
        filter("POINT(-3.790541 40.442113)", "is_within").check(0);
    }

    @Test
    public void testIntersectsPoint9() {
        filter("POINT(-3.794575 40.443159)", "intersects").checkUnorderedColumns("place", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testContainsPoint9() {
        filter("POINT(-3.794575 40.443159)", "contains").checkUnorderedColumns("place", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint9() {
        filter("POINT(-3.794575 40.443159)", "is_within").check(0);
    }

    @Test
    public void testIntersectsPoint10() {
        filter("POINT(-3.795648 40.441264)", "intersects").checkUnorderedColumns("place", "SHAPE_2");
    }

    @Test
    public void testContainsPoint10() {
        filter("POINT(-3.795648 40.441264)", "contains").checkUnorderedColumns("place", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint10() {
        filter("POINT(-3.795648 40.441264)", "is_within").check(0);
    }

    @Test
    public void testIntersectsPoint11() {
        filter("POINT(-3.796248 40.442342)", "intersects").checkUnorderedColumns("place", "SHAPE_2");
    }

    @Test
    public void testContainsPoint11() {
        filter("POINT(-3.796248 40.442342)", "contains").checkUnorderedColumns("place", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint11() {
        filter("POINT(-3.796248 40.442342)", "is_within").check(0);
    }

    @Test
    public void testIntersectsPoint12() {
        filter("POINT(-3.795862 40.440676)", "intersects").checkUnorderedColumns("place", "SHAPE_2");
    }

    @Test
    public void testContainsPoint12() {
        filter("POINT(-3.795862 40.440676)", "contains").checkUnorderedColumns("place", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint12() {
        filter("POINT(-3.795862 40.440676)", "is_within").check(0);
    }

    @Test
    public void testStarShapedIntersectsQuery() {
        filter("POLYGON((-3.798180 40.444563,-3.789082 40.442473,-3.796077 40.437835, -3.793201 40.441427," +
               "-3.798180 40.444563))", "intersects").checkUnorderedColumns("place", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testStarShapedIntersectsQuery2() {
        filter("POLYGON((-3.8012266 40.4384634, -3.7821293000000002 40.44178680000001, -3.7937164 40.4453468, " +
               "-3.7937164 40.453054, -3.8012266 40.4384634))", "intersects")
                .checkUnorderedColumns("place", "SHAPE_1", "SHAPE_2", "SHAPE_3");
    }

    @Test
    public void testConcaveShapesIntersectsQuery() {
        filter("POLYGON((-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001,-3.785691299999999 " +
               "40.445020199999995,-3.781742999999999 40.43427419999999,-3.7777947999999997 40.4497883,-3.8094234 " +
               "40.44858,-3.8033294999999994 40.4349602))", "intersects")
                .checkUnorderedColumns("place", "SHAPE_2", "SHAPE_3");
    }

    @Test
    public void testStarShapedContainsQuery() {
        filter("POLYGON((-3.798180 40.444563,-3.789082 40.442473,-3.796077 40.437835, -3.793201 40.441427," +
               "-3.798180 40.444563))", "contains").checkUnorderedColumns("place", "SHAPE_1");
    }

    @Test
    public void testStarShapedContainsQuery2() {
        filter("POLYGON((-3.8012266 40.4384634, -3.7821293000000002 40.44178680000001, " +
               "-3.7937164 40.4453468, -3.7937164 40.453054, -3.8012266 40.4384634))", "contains")
                .checkUnorderedColumns("place", "SHAPE_2");
    }

    @Test
    public void testConcaveShapesContainsQuery() {
        filter("POLYGON((-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001," +
               "-3.785691299999999 40.445020199999995,-3.781742999999999 40.43427419999999," +
               "-3.7777947999999997 40.4497883,-3.8094234 40.44858,-3.8033294999999994 40.4349602))", "contains")
                .checkUnorderedColumns("place", "SHAPE_3");
    }

    @Test
    public void testStarShapedIsWithinQuery() {
        filter("POLYGON((-3.798180 40.444563,-3.789082 40.442473,-3.796077 40.437835,-3.793201 40.441427," +
               "-3.798180 40.444563))", "is_within").check(1);
    }

    @Test
    public void testStarShapedIsWithinQuery2() {
        filter("POLYGON((-3.8012266 40.4384634, -3.7821293000000002 40.44178680000001, -3.7937164 40.4453468, " +
               "-3.7937164 40.453054, -3.8012266 40.4384634))", "is_within").check(1);
    }

    @Test
    public void testConcaveShapesIsWithinQuery() {
        filter("POLYGON((-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001,-3.785691299999999 " +
               "40.445020199999995,-3.781742999999999 40.43427419999999,-3.7777947999999997 40.4497883,-3.8094234 " +
               "40.44858,-3.8033294999999994 40.4349602))",
               "is_within").check(1);
    }

    @Test
    public void testBufferIntersectsShape() {
        filter(buffer("LINESTRING(-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001," +
                      "-3.785691299999999 40.445020199999995)").maxDistance("500m"), "intersects")
                .checkUnorderedColumns("place", "SHAPE_1", "SHAPE_2", "SHAPE_3");
    }

    @Test
    public void testBufferContainsShape() {
        filter(buffer("LINESTRING(-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001," +
                      "-3.785691299999999 40.445020199999995)").maxDistance("500m"), "contains").check(0);
    }

    @Test
    public void testBufferIsWithinShape() {
        filter(buffer("LINESTRING(-3.8033294999999994 40.4349602,-3.7986946 40.44511810000001," +
                      "-3.785691299999999 40.445020199999995)").maxDistance("500m"), "is_within").check(0);
    }

    @Test
    public void testLine1Intersects() {
        filter("LINESTRING (30 10, 10 30, 40 40)", "intersects")
                .checkUnorderedColumns("place",
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
        filter("LINESTRING (30 10, 10 30, 40 40)", "contains").checkUnorderedColumns("place", "LINE_1", "LINE_2");
    }

    @Test
    public void testLine1IsWithin() {
        filter("LINESTRING (30 10, 10 30, 40 40)", "is_within").check(1);
    }

    @Test
    public void testLine2Intersects() {
        filter("LINEARRING(30 10, 10 30, 40 40,30 10)", "intersects")
                .checkUnorderedColumns("place",
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
        filter("LINEARRING(30 10, 10 30, 40 40,30 10)", "contains").checkUnorderedColumns("place", "LINE_2");
    }

    @Test
    public void testLine2IsWithin() {
        filter("LINEARRING(30 10, 10 30, 40 40,30 10)", "is_within").checkUnorderedColumns("place", "LINE_1", "LINE_2");
    }

    @Test
    public void testPolygonIntersects() {
        filter("POLYGON((0.0 0.0,0.0 10.0,10.0 0.0,0.0 0.0))", "intersects")
                .checkUnorderedColumns("place", "POLYGON_1");
    }

    @Test
    public void testPolygonContains() {
        filter("POLYGON((0.0 0.0,0.0 10.0,10.0 0.0,0.0 0.0))", "contains").checkUnorderedColumns("place", "POLYGON_1");
    }

    @Test
    public void testPolygonIsWithin() {
        filter("POLYGON((0.0 0.0,0.0 10.0,10.0 0.0,0.0 0.0))", "is_within").check(1);
    }

    @Test
    public void testPolygon2Intersects() {
        filter("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))", "intersects")
                .checkUnorderedColumns("place",
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
        filter("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))", "contains")
                .checkUnorderedColumns("place", "POLYGON_2");
    }

    @Test
    public void testPolygon2IsWithin() {
        filter("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))", "is_within").check(1);
    }

    @Test
    public void testMultipointIntersects() {
        filter("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))", "intersects")
                .checkUnorderedColumns("place",
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
        filter("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))", "contains")
                .checkUnorderedColumns("place", "MULTIPOINT_1", "MULTIPOINT_2");
    }

    @Test
    public void testMultipointIsWithin() {
        filter("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))", "is_within")
                .checkUnorderedColumns("place", "MULTIPOINT_1", "MULTIPOINT_2");
    }

    @Test
    public void testMultipoint2Intersects() {
        filter("MULTIPOINT (10 40, 40 30, 20 20, 30 10)", "intersects")
                .checkUnorderedColumns("place",
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
        filter("MULTIPOINT (10 40, 40 30, 20 20, 30 10)", "contains")
                .checkUnorderedColumns("place", "MULTIPOINT_1", "MULTIPOINT_2");
    }

    @Test
    public void testMultipoint2IsWithin() {
        filter("MULTIPOINT (10 40, 40 30, 20 20, 30 10)", "is_within")
                .checkUnorderedColumns("place", "MULTIPOINT_1", "MULTIPOINT_2");
    }

    @Test
    public void testMultilineIntersects() {
        filter("MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))", "intersects")
                .checkUnorderedColumns("place",
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
        filter("MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))", "contains")
                .checkUnorderedColumns("place", "MULTILINESTRING_1");
    }

    @Test
    public void testMultilineIsWithin() {
        filter("MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))", "is_within").check(1);
    }

    @Test
    public void testMultipolygonIntersects() {
        filter("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))", "intersects")
                .checkUnorderedColumns("place",
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
        filter("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))",
               "contains").checkUnorderedColumns("place", "MULTIPOLYGON_1");
    }

    @Test
    public void testMultipolygonIsWithin() {
        filter("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))",
               "is_within").check(1);
    }

    @Test
    public void testMultipolygon2Intersects() {
        filter("MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35)," +
               "(30 20, 20 15, 20 25, 30 20)))", "intersects")
                .checkUnorderedColumns("place",
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
        filter("MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35)," +
               "(30 20, 20 15, 20 25, 30 20)))", "contains").checkUnorderedColumns("place", "MULTIPOLYGON_2");
    }

    @Test
    public void testMultipolygon2IsWithin() {
        filter("MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35)," +
               "(30 20, 20 15, 20 25, 30 20)))", "is_within").check(1);
    }
}
