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

import java.util.LinkedHashMap;
import java.util.Map;

import static com.stratio.cassandra.lucene.builder.Builder.*;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoShapeSearchOverGeoPointsAT extends BaseAT {

    protected static CassandraUtils utils;

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
        utils = CassandraUtils.builder("shape_search_over_points")
                              .withPartitionKey("place")
                              .withClusteringKey()
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
        utils.dropIndex().dropTable().dropKeyspace();
    }

    private void test(String shape, String operation, String... expected) {
        utils.filter(geoShape("location", shape).operation(operation)).checkStringColumnWithoutOrder("place", expected);
    }

    private void test(String shape, GeoTransformation transformation, String operation, String... expected) {
        utils.filter(geoShape("location", shape).transform(transformation).operation(operation))
             .checkStringColumnWithoutOrder("place", expected);
    }

    @Test
    public void testStarShapedIntersectsQuery() {
        test("POLYGON((-3.79818 40.44456,-3.78908 40.44247,-3.796077 40.43783, -3.79320 40.44142,-3.79818 40.44456))",
             "intersects",
             "POINT_7",
             "POINT_8",
             "POINT_9");
    }

    @Test
    public void testStarShapedIntersectsQuery2() {
        test("POLYGON((-3.80122 40.43846, -3.78212 40.44178, -3.79371 40.44534, -3.79371 40.45305, -3.80122 40.43846))",
             "intersects",
             "POINT_7",
             "POINT_8",
             "POINT_9",
             "POINT_10",
             "POINT_11",
             "POINT_12");
    }

    @Test
    public void testConcaveShapesIntersectsQuery() {
        test("POLYGON((-3.80332 40.43496,-3.79869 40.44511,-3.78569 40.44502,-3.78174 40.43427,-3.77779 40.44978,-3.80942 40.448,-3.80332 40.43496))",
             "intersects",
             "POINT_3",
             "POINT_4",
             "POINT_5",
             "POINT_6");
    }

    @Test
    public void testStarShapedContainsQuery() {
        test("POLYGON((-3.79818 40.44456,-3.78908 40.44247,-3.79607 40.43783, -3.79320 40.44142,-3.79818 40.44456))",
             "contains");
    }

    @Test
    public void testStarShapedContainsQuery2() {
        test("POLYGON((-3.80122 40.43846, -3.78212 40.44178, -3.79371 40.44534, -3.79371 40.45305, -3.80122 40.43846))",
             "contains");
    }

    @Test
    public void testConcaveShapesContainsQuery() {
        test("POLYGON((-3.80332 40.43496,-3.79869 40.44511,-3.78569 40.44502,-3.78174 40.43427,-3.77779 40.44978,-3.80942 40.448,-3.80332 40.43496))",
             "contains");

    }

    @Test
    public void testStarShapedIsWithinQuery() {
        test("POLYGON((-3.798180 40.444563,-3.789082 40.442473,-3.796077 40.437835, -3.793201 40.441427,-3.798180 40.444563))",
             "is_within",
             "POINT_7",
             "POINT_8",
             "POINT_9");
    }

    @Test
    public void testStarShapedIsWithinQuery2() {
        test("POLYGON((-3.80122 40.43846, -3.78212 40.44178, -3.79371 40.44534, -3.79371 40.45305, -3.80122 40.43846))",
             "is_within",
             "POINT_7",
             "POINT_8",
             "POINT_9",
             "POINT_10",
             "POINT_11",
             "POINT_12");
    }

    @Test
    public void testConcaveShapesIsWithinQuery() {
        test("POLYGON((-3.80332 40.43496,-3.79869 40.44511,-3.78569 40.44502,-3.78174 40.43427,-3.77779 40.44978,-3.80942 40.448,-3.80332 40.43496))",
             "is_within",
             "POINT_3",
             "POINT_4",
             "POINT_5",
             "POINT_6");
    }

    @Test
    public void testBufferShape() {
        test("LINESTRING(-3.8033294 40.4349602,-3.7986946 40.4451181, -3.7856912 40.4450201)",
             bufferGeoTransformation().maxDistance("500m"),
             "intersects",
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

    @Test
    public void testBBoxPoint() {
        test("POINT(-3.793030 40.435450)", bboxGeoTransformation(), "intersects", "POINT_1");
    }

    @Test
    public void testBBoxLine() {
        test("LINESTRING(-3.80 40.43,-3.78 40.44)", bboxGeoTransformation(), "intersects", "POINT_1", "POINT_2");
    }

    @Test
    public void testBBoxPolygon() {
        test("POLYGON((-3.80 40.43, -3.78 40.43, -3.78 40.44, -3.80 40.43))",
             bboxGeoTransformation(),
             "intersects",
             "POINT_1",
             "POINT_2");
    }

}
