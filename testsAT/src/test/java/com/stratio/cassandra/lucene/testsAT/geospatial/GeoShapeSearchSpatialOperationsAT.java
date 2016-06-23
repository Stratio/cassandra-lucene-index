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

import com.stratio.cassandra.lucene.builder.search.condition.GeoShapeCondition;
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
public class GeoShapeSearchSpatialOperationsAT extends BaseAT {

    private static String shape_1 = "POLYGON((0 0,1 0, 1 1,0 1,0 0))";
    private static String shape_2 = "POLYGON((-0.1 -0.1,1.1 -0.1, 1.1 1.1,-0.1 1.1,-0.1 -0.1))";
    private static String shape_3 = "POLYGON((1 0,2 0,2 1,1 1,1 0))";
    private static String shape_4 = "POLYGON((1 1,2 1,2 2,1 2,1 1))";
    private static String shape_5 = "POLYGON((2 2,3 2,3 3,2 3,2 2))";
    private static String shape_6 = "POLYGON((0 0,2 0,2 1,0 1,0 0))";
    private static String shape_7 = "POLYGON((0 1,1 1,1 2,0 2,0 1))";
    private static String shape_8 = "POLYGON((0 0,1 0,1 2,0 2,0 0))";
    private static String shape_9 = "POLYGON((1 0,2 0,1 2,1 2,1 0))";
    private static String shape_10 = "POLYGON((0 1,2 1,2 2,0 2,0 1))";

    public static final Map<String, String> data1, data2, data3, data4, data5, data6, data7, data8, data9;
    protected static CassandraUtils cassandraUtils;

    static {
        data1 = new LinkedHashMap<>();
        data1.put("id", "'1'");
        data1.put("search_case", "1");
        data1.put("identity", "'1'");
        data1.put("shape", "'" + shape_1 + "'");

        data2 = new LinkedHashMap<>();
        data2.put("id", "'2'");
        data2.put("search_case", "2");
        data2.put("identity", "'1'");
        data2.put("shape", "'" + shape_1 + "'");

        data3 = new LinkedHashMap<>();
        data3.put("id", "'3'");
        data3.put("search_case", "2");
        data3.put("identity", "'2'");
        data3.put("shape", "'" + shape_2 + "'");

        data4 = new LinkedHashMap<>();
        data4.put("id", "'4'");
        data4.put("search_case", "3");
        data4.put("identity", "'1'");
        data4.put("shape", "'" + shape_1 + "'");

        data5 = new LinkedHashMap<>();
        data5.put("id", "'5'");
        data5.put("search_case", "3");
        data5.put("identity", "'3'");
        data5.put("shape", "'" + shape_3 + "'");

        data6 = new LinkedHashMap<>();
        data6.put("id", "'6'");
        data6.put("search_case", "4");
        data6.put("identity", "'1'");
        data6.put("shape", "'" + shape_1 + "'");

        data7 = new LinkedHashMap<>();
        data7.put("id", "'7'");
        data7.put("search_case", "4");
        data7.put("identity", "'4'");
        data7.put("shape", "'" + shape_4 + "'");

        data8 = new LinkedHashMap<>();
        data8.put("id", "'8'");
        data8.put("search_case", "5");
        data8.put("identity", "'1'");
        data8.put("shape", "'" + shape_1 + "'");

        data9 = new LinkedHashMap<>();
        data9.put("id", "'9'");
        data9.put("search_case", "5");
        data9.put("identity", "'5'");
        data9.put("shape", "'" + shape_5 + "'");

    }

    @BeforeClass
    public static void setUpSuite() {
        cassandraUtils = CassandraUtils.builder("search")
                                       .withPartitionKey("id")
                                       .withClusteringKey()
                                       .withColumn("lucene", "text", null)
                                       .withColumn("id", "text", null)
                                       .withColumn("identity", "text", null)
                                       .withColumn("search_case", "int", integerMapper())
                                       .withColumn("shape", "text", geoShapeMapper())
                                       .build()
                                       .createKeyspace()
                                       .createTable()
                                       .createIndex()
                                       .insert(data1, data2, data3, data4, data5, data6, data7, data8, data9)
                                       .refresh();
    }

    @AfterClass
    public static void tearDownSuite() {
        cassandraUtils.dropIndex().dropTable().dropKeyspace();
    }

    @Test
    public void testCase1() {
        GeoShapeCondition geoShape = geoShape("shape", shape_1);
        Integer search_case = 1;
        cassandraUtils
                .filter(bool().must(geoShape.operation("contains"), match("search_case", search_case)))
                .checkStringColumn("identity", "1")
                .filter(bool().must(geoShape.operation("intersects"), match("search_case", search_case)))
                .checkStringColumn("identity", "1")
                .filter(bool().must(geoShape.operation("is_within"), match("search_case", search_case)))
                .check(0);
        // index A, search A must return intersecs and Contains but no is_within
    }

    @Test
    public void testCase2() {
        //shape_1 is within shape_3 so must return
        GeoShapeCondition geoShape = geoShape("shape", shape_1);
        GeoShapeCondition geoShape2 = geoShape("shape", shape_2);
        Integer search_case = 2;
        cassandraUtils.filter(bool().must(geoShape.operation("contains"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "2")
                      .filter(bool().must(geoShape.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "2")
                      .filter(bool().must(geoShape.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape2.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "2")
                      .filter(bool().must(geoShape2.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "2")
                      .filter(bool().must(geoShape2.operation("is_within"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1");
    }

    @Test
    public void testCase3() {
        //shape_1 shares with shape_3 just one exterior border so must intersects in the two ways and no more
        GeoShapeCondition geoShape = geoShape("shape", shape_1);
        GeoShapeCondition geoShape3 = geoShape("shape", shape_3);
        Integer search_case = 3;
        cassandraUtils.filter(bool().must(geoShape.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "3")
                      .filter(bool().must(geoShape.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape3.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "3")
                      .filter(bool().must(geoShape3.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "3")
                      .filter(bool().must(geoShape3.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase4() {
        //shape_1 is within shape_3
        GeoShapeCondition geoShape1 = geoShape("shape", shape_1);
        GeoShapeCondition geoShape4 = geoShape("shape", shape_4);
        Integer search_case = 4;
        cassandraUtils.filter(bool().must(geoShape1.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape1.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "4")
                      .filter(bool().must(geoShape1.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape4.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "4")
                      .filter(bool().must(geoShape4.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "4")
                      .filter(bool().must(geoShape4.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase5() {
        //shape_1 disjoint shape5
        GeoShapeCondition geoShape1 = geoShape("shape", shape_1);
        GeoShapeCondition geoShape5 = geoShape("shape", shape_5);
        Integer search_case = 5;
        cassandraUtils.filter(bool().must(geoShape1.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape1.operation("intersects"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape1.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape5.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "5")
                      .filter(bool().must(geoShape5.operation("intersects"), match("search_case", search_case)))
                      .checkStringColumn("identity", "5")
                      .filter(bool().must(geoShape5.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase1WithDifference() {
        GeoShapeCondition geoShape6 = geoShape("shape", shape_6).transform(differenceGeoTransformation(shape_3));
        Integer search_case = 1;
        cassandraUtils.filter(bool().must(geoShape6.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape6.operation("intersects"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape6.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase2WithDifference() {
        //shape_1 (2) is within shape_3 so must return
        GeoShapeCondition geoShape6 = geoShape("shape", shape_6).transform(differenceGeoTransformation(shape_3));
        Integer search_case = 2;
        cassandraUtils.filter(bool().must(geoShape6.operation("contains"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "2")
                      .filter(bool().must(geoShape6.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "2")
                      .filter(bool().must(geoShape6.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase3WithDifference() {
        //shape_1 shares with shape_3 just one exterior border so must intersects in the two ways and no more
        GeoShapeCondition geoShape6_3 = geoShape("shape", shape_6).transform(differenceGeoTransformation(shape_3));
        GeoShapeCondition geoShape6_1 = geoShape("shape", shape_6).transform(differenceGeoTransformation(shape_1));
        Integer search_case = 3;
        cassandraUtils.filter(bool().must(geoShape6_3.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape6_3.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "3")
                      .filter(bool().must(geoShape6_3.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape6_1.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "3")
                      .filter(bool().must(geoShape6_1.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "3")
                      .filter(bool().must(geoShape6_1.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase4WithDifference() {
        //shape_1 (2) is within shape_3
        GeoShapeCondition geoShape6_3 = geoShape("shape", shape_6).transform(differenceGeoTransformation(shape_3));
        GeoShapeCondition geoShape9_3 = geoShape("shape", shape_9).transform(differenceGeoTransformation(shape_3));
        Integer search_case = 4;
        cassandraUtils.filter(bool().must(geoShape6_3.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape6_3.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "4")
                      .filter(bool().must(geoShape6_3.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape9_3.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "4")
                      .filter(bool().must(geoShape9_3.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "4")
                      .filter(bool().must(geoShape9_3.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase5WithDifference() {
        //shape_1 disjoint shape5
        GeoShapeCondition geoShape6_3 = geoShape("shape", shape_6).transform(differenceGeoTransformation(shape_3));
        Integer search_case = 5;
        cassandraUtils.filter(bool().must(geoShape6_3.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape6_3.operation("intersects"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape6_3.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    //Intersection shape1= SHAPE_6 Intersectoion SHAPE_8, intersection is idempotent
    @Test
    public void testCase1WithIntersection() {
        // index A, search A must return intersecs and Contains but no is_within
        GeoShapeCondition geoShape6_8 = geoShape("shape", shape_6).transform(intersectionGeoTransformation(shape_8));
        GeoShapeCondition geoShape8_6 = geoShape("shape", shape_8).transform(intersectionGeoTransformation(shape_6));
        Integer search_case = 1;
        cassandraUtils.filter(bool().must(geoShape6_8.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape6_8.operation("intersects"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape6_8.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape8_6.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape8_6.operation("intersects"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape8_6.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase2WithIntersection() {
        //shape_1 (2) is within shape_3 so must return
        GeoShapeCondition geoShape6_8 = geoShape("shape", shape_6).transform(intersectionGeoTransformation(shape_8));
        GeoShapeCondition geoShape8_6 = geoShape("shape", shape_8).transform(intersectionGeoTransformation(shape_6));
        Integer search_case = 2;
        cassandraUtils.filter(bool().must(geoShape6_8.operation("contains"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "2")
                      .filter(bool().must(geoShape6_8.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "2")
                      .filter(bool().must(geoShape6_8.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape8_6.operation("contains"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "2")
                      .filter(bool().must(geoShape8_6.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "2")
                      .filter(bool().must(geoShape8_6.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase3WithIntersection() {
        //shape_1 shares with shape_3 just one exterior border so must intersects in the two ways and no more
        GeoShapeCondition geoShape6_8 = geoShape("shape", shape_6).transform(intersectionGeoTransformation(shape_8));
        GeoShapeCondition geoShape8_6 = geoShape("shape", shape_8).transform(intersectionGeoTransformation(shape_6));
        GeoShapeCondition geoShape6_9 = geoShape("shape", shape_6).transform(intersectionGeoTransformation(shape_9));
        GeoShapeCondition geoShape9_6 = geoShape("shape", shape_9).transform(intersectionGeoTransformation(shape_6));
        Integer search_case = 3;
        cassandraUtils.filter(bool().must(geoShape6_8.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape6_8.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "3")
                      .filter(bool().must(geoShape6_8.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape8_6.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape8_6.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "3")
                      .filter(bool().must(geoShape8_6.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape6_9.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "3")
                      .filter(bool().must(geoShape6_9.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "3")
                      .filter(bool().must(geoShape6_9.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape9_6.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "3")
                      .filter(bool().must(geoShape9_6.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "3")
                      .filter(bool().must(geoShape9_6.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase4WithIntersection() {
        //shape_1 (2) is within shape_3
        GeoShapeCondition geoShape6_8 = geoShape("shape", shape_6).transform(intersectionGeoTransformation(shape_8));
        GeoShapeCondition geoShape8_6 = geoShape("shape", shape_8).transform(intersectionGeoTransformation(shape_6));
        GeoShapeCondition geoShape9_10 = geoShape("shape", shape_9).transform(intersectionGeoTransformation(shape_10));
        GeoShapeCondition geoShape10_9 = geoShape("shape", shape_10).transform(intersectionGeoTransformation(shape_9));
        Integer search_case = 4;
        cassandraUtils.filter(bool().must(geoShape6_8.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape6_8.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "4")
                      .filter(bool().must(geoShape6_8.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape8_6.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape8_6.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "4")
                      .filter(bool().must(geoShape8_6.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape9_10.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "4")
                      .filter(bool().must(geoShape9_10.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "4")
                      .filter(bool().must(geoShape9_10.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape10_9.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "4")
                      .filter(bool().must(geoShape10_9.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "4")
                      .filter(bool().must(geoShape10_9.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase5WithIntersection() {
        //shape_1 disjoint shape5
        GeoShapeCondition geoShape6_8 = geoShape("shape", shape_6).transform(intersectionGeoTransformation(shape_8));
        GeoShapeCondition geoShape8_6 = geoShape("shape", shape_8).transform(intersectionGeoTransformation(shape_6));
        Integer search_case = 5;
        cassandraUtils.filter(bool().must(geoShape6_8.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape6_8.operation("intersects"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape6_8.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape8_6.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape8_6.operation("intersects"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape8_6.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase1WithUnion() {
        // index A, search A must return intersecs and Contains but no is_within
        GeoShapeCondition geoShape1_3_8 = geoShape("shape", shape_1).transform(unionGeoTransformation(shape_3),
                                                                               intersectionGeoTransformation(shape_8));
        GeoShapeCondition geoShape1_7_6 = geoShape("shape", shape_1).transform(unionGeoTransformation(shape_7),
                                                                               intersectionGeoTransformation(shape_6));
        Integer search_case = 1;
        cassandraUtils.filter(bool().must(geoShape1_3_8.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape1_3_8.operation("intersects"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape1_3_8.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape1_7_6.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape1_7_6.operation("intersects"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape1_7_6.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase2WithUnion() {
        //shape_1 (2) is within shape_3 so must return
        GeoShapeCondition geoShape1_3_8 = geoShape("shape", shape_1).transform(unionGeoTransformation(shape_3),
                                                                               intersectionGeoTransformation(shape_8));
        GeoShapeCondition geoShape1_7_6 = geoShape("shape", shape_1).transform(unionGeoTransformation(shape_7),
                                                                               intersectionGeoTransformation(shape_6));
        Integer search_case = 2;
        cassandraUtils.filter(bool().must(geoShape1_3_8.operation("contains"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "2")
                      .filter(bool().must(geoShape1_3_8.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "2")
                      .filter(bool().must(geoShape1_3_8.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape1_7_6.operation("contains"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "2")
                      .filter(bool().must(geoShape1_7_6.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "2")
                      .filter(bool().must(geoShape1_7_6.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase3WithUnion() {
        //shape_1 shares with shape_3 just one exterior border so must intersects in the two ways and no more
        GeoShapeCondition geoShape1_3_8 = geoShape("shape", shape_1).transform(unionGeoTransformation(shape_3),
                                                                               intersectionGeoTransformation(shape_8));
        GeoShapeCondition geoShape1_7_6 = geoShape("shape", shape_1).transform(unionGeoTransformation(shape_7),
                                                                               intersectionGeoTransformation(shape_6));
        GeoShapeCondition geoShape1_3_9 = geoShape("shape", shape_1).transform(unionGeoTransformation(shape_3),
                                                                               intersectionGeoTransformation(shape_9));
        GeoShapeCondition geoShape9_6 = geoShape("shape", shape_9).transform(intersectionGeoTransformation(shape_6));
        Integer search_case = 3;
        cassandraUtils.filter(bool().must(geoShape1_3_8.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape1_3_8.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "3")
                      .filter(bool().must(geoShape1_3_8.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape1_7_6.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape1_7_6.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "3")
                      .filter(bool().must(geoShape1_7_6.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape1_3_9.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "3")
                      .filter(bool().must(geoShape1_3_9.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "3")
                      .filter(bool().must(geoShape1_3_9.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape9_6.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "3")
                      .filter(bool().must(geoShape9_6.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "3")
                      .filter(bool().must(geoShape9_6.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase4WithUnion() {
        //shape_1 (2) is within shape_3
        GeoShapeCondition geoShape1_3_8 = geoShape("shape", shape_1).transform(unionGeoTransformation(shape_3),
                                                                               intersectionGeoTransformation(shape_8));
        GeoShapeCondition geoShape1_7_6 = geoShape("shape", shape_1).transform(unionGeoTransformation(shape_7),
                                                                               intersectionGeoTransformation(shape_6));
        GeoShapeCondition geoShape9_10 = geoShape("shape", shape_9).transform(intersectionGeoTransformation(shape_10));
        GeoShapeCondition geoShape4_7_9 = geoShape("shape", shape_4).transform(unionGeoTransformation(shape_7),
                                                                               intersectionGeoTransformation(shape_9));
        Integer search_case = 4;
        cassandraUtils.filter(bool().must(geoShape1_3_8.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape1_3_8.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "4")
                      .filter(bool().must(geoShape1_3_8.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape1_7_6.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape1_7_6.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "4")
                      .filter(bool().must(geoShape1_7_6.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape9_10.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "4")
                      .filter(bool().must(geoShape9_10.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "4")
                      .filter(bool().must(geoShape9_10.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape4_7_9.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "4")
                      .filter(bool().must(geoShape4_7_9.operation("intersects"), match("search_case", search_case)))
                      .checkUnorderedStringColumns("identity", "1", "4")
                      .filter(bool().must(geoShape4_7_9.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }

    @Test
    public void testCase5WithUnion() {
        //shape_1 disjoint shape5
        GeoShapeCondition geoShape1_3_8 = geoShape("shape", shape_1).transform(unionGeoTransformation(shape_3),
                                                                               intersectionGeoTransformation(shape_8));
        GeoShapeCondition geoShape1_7_6 = geoShape("shape", shape_1).transform(unionGeoTransformation(shape_7),
                                                                               intersectionGeoTransformation(shape_6));
        Integer search_case = 5;
        cassandraUtils.filter(bool().must(geoShape1_3_8.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape1_3_8.operation("intersects"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape1_3_8.operation("is_within"), match("search_case", search_case)))
                      .check(0)
                      .filter(bool().must(geoShape1_7_6.operation("contains"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape1_7_6.operation("intersects"), match("search_case", search_case)))
                      .checkStringColumn("identity", "1")
                      .filter(bool().must(geoShape1_7_6.operation("is_within"), match("search_case", search_case)))
                      .check(0);
    }
}