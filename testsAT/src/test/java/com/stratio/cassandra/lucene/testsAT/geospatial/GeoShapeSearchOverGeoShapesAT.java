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

import com.stratio.cassandra.lucene.builder.common.GeoTransformation;
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
public class GeoShapeSearchOverGeoShapesAT extends BaseAT {

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
                  "'POLYGON((-3.8012266 40.4384634, -3.7821293000000002 40.4417868," +
                  "-3.7937164 40.4453468, -3.7937164 40.453054, -3.8012266 40.4384634))'");

        data3 = new LinkedHashMap<>();
        data3.put("place", "'SHAPE_3'");
        data3.put("shape", "'POLYGON((-3.803329494 40.4349602,-3.7986946 40.4451181," +
                           "-3.78569129 40.44502019,-3.78174299 40.4342741," +
                           "-3.777794797 40.4497883,-3.8094234 40.44858,-3.803329494 40.4349602))'");

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
        utils = CassandraUtils.builder("shape_search_over_shapes")
                              .withPartitionKey("place")
                              .withClusteringKey()
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
        utils.dropIndex().dropTable().dropKeyspace();
    }

    private void test(String shape, String operation, String... expected) {
        utils.filter(geoShape("shape", shape).operation(operation)).checkStringColumnWithoutOrder("place", expected);
    }

    private void test(String shape, GeoTransformation transformation, String operation, String... expected) {
        utils.filter(geoShape("shape", shape).transform(transformation).operation(operation))
             .checkStringColumnWithoutOrder("place", expected);
    }

    @Test
    public void testIntersectsPoint1() {
        test("POINT(-3.793030 40.435450)", "intersects");

    }

    @Test
    public void testContainsPoint1() {
        test("POINT(-3.793030 40.435450)", "contains");
    }

    @Test
    public void testIsWithinPoint1() {
        test("POINT(-3.793030 40.435450)", "is_within");
    }

    @Test
    public void testIntersectsPoint2() {
        test("POINT(-3.789382 40.436169)", "intersects");

    }

    @Test
    public void testContainsPoint2() {
        test("POINT(-3.789382 40.436169)", "contains");

    }

    @Test
    public void testIsWithinPoint2() {
        test("POINT(-3.789382 40.436169)", "is_within");

    }

    @Test
    public void testIntersectsPoint3() {
        test("POINT(-3.789725 40.446751)", "intersects", "SHAPE_3");
    }

    @Test
    public void testContainsPoint3() {
        test("POINT(-3.789725 40.446751)", "contains", "SHAPE_3");
    }

    @Test
    public void testIsWithinPoint3() {
        test("POINT(-3.789725 40.446751)", "is_within");
    }

    @Test
    public void testIntersectsPoint4() {
        test("POINT(-3.792343 40.446522)", "intersects", "SHAPE_3");
    }

    @Test
    public void testContainsPoint4() {
        test("POINT(-3.792343 40.446522)", "contains", "SHAPE_3");
    }

    @Test
    public void testIsWithinPoint4() {
        test("POINT(-3.792343 40.446522)", "is_within");
    }

    @Test
    public void testIntersectsPoint5() {
        test("POINT(-3.804402 40.444040)", "intersects", "SHAPE_3");
    }

    @Test
    public void testContainsPoint5() {
        test("POINT(-3.804402 40.444040)", "contains", "SHAPE_3");
    }

    @Test
    public void testIsWithinPoint5() {
        test("POINT(-3.804402 40.444040)", "is_within");
    }

    @Test
    public void testIntersectsPoint6() {
        test("POINT(-3.803630 40.436724)", "intersects", "SHAPE_3");
    }

    @Test
    public void testContainsPoint6() {
        test("POINT(-3.803630 40.436724)", "contains", "SHAPE_3");
    }

    @Test
    public void testIsWithinPoint6() {
        test("POINT(-3.803630 40.436724)", "is_within");
    }

    @Test
    public void testIntersectsPoint7() {
        test("POINT(-3.792472 40.440938)", "intersects", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testContainsPoint7() {
        test("POINT(-3.792472 40.440938)", "contains", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint7() {
        test("POINT(-3.792472 40.440938)", "is_within");
    }

    @Test
    public void testIntersectsPoint8() {
        test("POINT(-3.790541 40.442113)", "intersects", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testContainsPoint8() {
        test("POINT(-3.790541 40.442113)", "contains", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint8() {
        test("POINT(-3.790541 40.442113)", "is_within");
    }

    @Test
    public void testIntersectsPoint9() {
        test("POINT(-3.794575 40.443159)", "intersects", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testContainsPoint9() {
        test("POINT(-3.794575 40.443159)", "contains", "SHAPE_1", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint9() {
        test("POINT(-3.794575 40.443159)", "is_within");
    }

    @Test
    public void testIntersectsPoint10() {
        test("POINT(-3.795648 40.441264)", "intersects", "SHAPE_2");
    }

    @Test
    public void testContainsPoint10() {
        test("POINT(-3.795648 40.441264)", "contains", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint10() {
        test("POINT(-3.795648 40.441264)", "is_within");
    }

    @Test
    public void testIntersectsPoint11() {
        test("POINT(-3.796248 40.442342)", "intersects", "SHAPE_2");
    }

    @Test
    public void testContainsPoint11() {
        test("POINT(-3.796248 40.442342)", "contains", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint11() {
        test("POINT(-3.796248 40.442342)", "is_within");
    }

    @Test
    public void testIntersectsPoint12() {
        test("POINT(-3.795862 40.440676)", "intersects", "SHAPE_2");
    }

    @Test
    public void testContainsPoint12() {
        test("POINT(-3.795862 40.440676)", "contains", "SHAPE_2");
    }

    @Test
    public void testIsWithinPoint12() {
        test("POINT(-3.795862 40.440676)", "is_within");
    }

    @Test
    public void testStarShapedIntersectsQuery() {
        test("POLYGON((-3.798180 40.444563,-3.789082 40.442473,-3.796077 40.437835, -3.793201 40.441427,-3.798180 40.444563))",
             "intersects",
             "SHAPE_1",
             "SHAPE_2");
    }

    @Test
    public void testStarShapedIntersectsQuery2() {
        test("POLYGON((-3.8012266 40.4384634, -3.7821293 40.4417868, -3.7937164 40.4453468, -3.7937164 40.453054, -3.8012266 40.4384634))",
             "intersects",
             "SHAPE_1",
             "SHAPE_2",
             "SHAPE_3");
    }

    @Test
    public void testConcaveShapesIntersectsQuery() {
        test("POLYGON((-3.8033294 40.4349602,-3.7986946 40.4451181,-3.7856912 40.4450201,-3.7817429 40.4342741,-3.7777947 40.4497883,-3.8094234 40.44858,-3.8033294 40.4349602))",
             "intersects",
             "SHAPE_2",
             "SHAPE_3");

    }

    @Test
    public void testStarShapedContainsQuery() {
        test("POLYGON((-3.798180 40.444563,-3.789082 40.442473,-3.796077 40.437835, -3.793201 40.441427,-3.798180 40.444563))",
             "contains",
             "SHAPE_1");
    }

    @Test
    public void testStarShapedContainsQuery2() {
        test("POLYGON((-3.8012266 40.4384634, -3.7821293 40.4417868, -3.7937164 40.4453468, -3.7937164 40.453054, -3.8012266 40.4384634))",
             "contains",
             "SHAPE_2");
    }

    @Test
    public void testConcaveShapesContainsQuery() {
        test("POLYGON((-3.8033294 40.4349602,-3.7986946 40.4451181,-3.7856912 40.4450201,-3.7817429 40.4342741,-3.7777947 40.4497883,-3.8094234 40.44858,-3.8033294 40.4349602))",
             "contains",
             "SHAPE_3");
    }

    @Test
    public void testStarShapedIsWithinQuery() {
        test("POLYGON((-3.798180 40.444563,-3.789082 40.442473,-3.796077 40.437835, -3.793201 40.441427,-3.798180 40.444563))",
             "is_within");
    }

    @Test
    public void testStarShapedIsWithinQuery2() {
        test("POLYGON((-3.8012266 40.4384634, -3.7821293 40.4417868, -3.7937164 40.4453468, -3.7937164 40.453054, -3.8012266 40.4384634))",
             "is_within");
    }

    @Test
    public void testConcaveShapesIsWithinQuery() {
        test("POLYGON((-3.8033294 40.4349602,-3.7986946 40.4451181,-3.7856912 40.4450201,-3.7817429 40.4342741,-3.7777947 40.4497883,-3.8094234 40.44858,-3.8033294 40.4349602))",
             "is_within");

    }

    @Test
    public void testBufferIntersectsShape() {
        test("LINESTRING(-3.803329494 40.4349602,-3.7986946 40.4451181,-3.78569129 40.44502019)",
             bufferGeoTransformation().maxDistance("500m"),
             "intersects",
             "SHAPE_1",
             "SHAPE_2",
             "SHAPE_3");
    }

    @Test
    public void testBufferContainsShape() {
        test("LINESTRING(-3.803329494 40.4349602,-3.7986946 40.4451181,-3.78569129 40.44502019)",
             bufferGeoTransformation().maxDistance("500m"),
             "contains");

    }

    @Test
    public void testBufferIsWithinShape() {
        test("LINESTRING(-3.803329494 40.4349602,-3.7986946 40.4451181, -3.78569129 40.44502019)",
             bufferGeoTransformation().maxDistance("500m"),
             "is_within");
    }

    @Test
    public void testLine1Intersects() {
        test("LINESTRING (30 10, 10 30, 40 40)",
             "intersects",
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
        test("LINESTRING (30 10, 10 30, 40 40)", "contains", "LINE_1", "LINE_2");
    }

    @Test
    public void testLine1IsWithin() {
        test("LINESTRING (30 10, 10 30, 40 40)", "is_within");
    }

    @Test
    public void testLine2Intersects() {
        test("LINEARRING(30 10, 10 30, 40 40,30 10)",
             "intersects",
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
        test("LINEARRING(30 10, 10 30, 40 40,30 10)", "contains", "LINE_2");
    }

    @Test
    public void testLine2IsWithin() {
        test("LINEARRING(30 10, 10 30, 40 40,30 10)", "is_within");
    }

    @Test
    public void testPolygonIntersects() {
        test("POLYGON((0.0 0.0,0.0 10.0,10.0 0.0,0.0 0.0))", "intersects", "POLYGON_1");
    }

    @Test
    public void testPolygonContains() {
        test("POLYGON((0.0 0.0,0.0 10.0,10.0 0.0,0.0 0.0))", "contains", "POLYGON_1");
    }

    @Test
    public void testPolygonIsWithin() {
        test("POLYGON((0.0 0.0,0.0 10.0,10.0 0.0,0.0 0.0))", "is_within");
    }

    @Test
    public void testPolygon2Intersects() {
        test("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))",
             "intersects", "LINE_1",
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
        test("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))",
             "contains", "POLYGON_2");
    }

    @Test
    public void testPolygon2IsWithin() {
        test("POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))", "is_within");
    }

    @Test
    public void testMultipointIntersects() {
        test("MULTIPOINT (10 40, 40 30, 20 20, 30 10)", "intersects",
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
        test("MULTIPOINT (10 40, 40 30, 20 20, 30 10)", "contains", "MULTIPOINT_1", "MULTIPOINT_2");
    }

    @Test
    public void testMultipointIsWithin() {
        test("MULTIPOINT (10 40, 40 30, 20 20, 30 10)", "is_within");
    }

    @Test
    public void testMultipoint2Intersects() {
        test("MULTIPOINT (10 40, 40 30, 20 20, 30 10)",
             "intersects",
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
        test("MULTIPOINT (10 40, 40 30, 20 20, 30 10)", "contains", "MULTIPOINT_1", "MULTIPOINT_2");
    }

    @Test
    public void testMultipoint2IsWithin() {
        test("MULTIPOINT (10 40, 40 30, 20 20, 30 10)", "is_within");
    }

    @Test
    public void testMultilineIntersects() {
        test("MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))",
             "intersects",
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
        test("MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))", "contains", "MULTILINESTRING_1");
    }

    @Test
    public void testMultilineIsWithin() {
        test("MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))", "is_within");
    }

    @Test
    public void testMultipolygonIntersects() {
        test("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))",
             "intersects",
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
        test("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))",
             "contains",
             "MULTIPOLYGON_1");
    }

    @Test
    public void testMultipolygonIsWithin() {
        test("MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))", "is_within");
    }

    @Test
    public void testMultipolygon2Intersects() {
        test("MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))",
             "intersects",
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
        test("MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))",
             "contains",
             "MULTIPOLYGON_2");
    }

    @Test
    public void testMultipolygon2IsWithin() {
        test("MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))",
             "is_within");
    }
}
