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
package com.stratio.cassandra.lucene.search.condition;

import com.spatial4j.core.shape.jts.JtsGeometry;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.common.GeoDistance;
import com.stratio.cassandra.lucene.common.GeoOperation;
import com.stratio.cassandra.lucene.common.GeoTransformation;
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.ContainsPrefixTreeQuery;
import org.apache.lucene.spatial.prefix.IntersectsPrefixTreeQuery;
import org.apache.lucene.spatial.prefix.WithinPrefixTreeQuery;
import org.junit.Test;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static com.stratio.cassandra.lucene.search.SearchBuilders.geoShape;
import static com.stratio.cassandra.lucene.util.GeospatialUtilsJTS.geometry;
import static org.junit.Assert.*;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoShapeConditionTest extends AbstractConditionTest {

    private static final String WKT = "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))";

    @Test
    public void testConstructor() throws ParseException {
        List<GeoTransformation> transformationList = new ArrayList<>();
        transformationList.add(new GeoTransformation.Buffer(GeoDistance.parse("1m"), GeoDistance.parse("2m")));

        GeoShapeCondition condition = new GeoShapeCondition(0.1f,
                                                            "geo_point",
                                                            WKT,
                                                            GeoOperation.IS_WITHIN,
                                                            transformationList);

        JtsGeometry geo = geometry(WKT);
        assertEquals("Boost is not set", 0.1f, condition.boost, 0);
        assertEquals("Field is not set", "geo_point", condition.field);
        assertEquals("Geometry is not set", geo, condition.geometry);
        assertEquals("Operation is not set", GeoOperation.IS_WITHIN, condition.operation);
        assertEquals("Transformations is not set", transformationList, condition.transformations);
    }

    @Test
    public void testConstructorWithDefaults() throws ParseException {
        GeoShapeCondition condition = new GeoShapeCondition(null, "geo_point", WKT, null, null);

        assertNull("Boost is not set to default", condition.boost);
        assertEquals("Field is not set", "geo_point", condition.field);

        JtsGeometry geo = geometry(WKT);

        assertEquals("Geometry is not set", geo, condition.geometry);
        assertEquals("Operation is not set", GeoShapeCondition.DEFAULT_OPERATION, condition.operation);
        assertEquals("Transformations is not set", new ArrayList(), condition.transformations);

    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullField() {
        new GeoShapeCondition(null, null, WKT, null, null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyField() {
        new GeoShapeCondition(null, "", WKT, null, null);

    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankField() {
        new GeoShapeCondition(null, " ", WKT, null, null);

    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullGeometry() {
        new GeoShapeCondition(null, "geo_point", null, null, null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithInvalidGeometry() {
        new GeoShapeCondition(null, "geo_point", "POLYGONS((1 1,5 1,5 5,1 5,1 1))", null, null);

    }

    @Test
    public void testQueryIsWithIn() {
        Schema schema = schema().mapper("geo_point", geoPointMapper("lat", "lon").maxLevels(8)).build();

        List<GeoTransformation> transformations = new ArrayList<>();
        transformations.add(new GeoTransformation.Buffer(GeoDistance.parse("1m"), GeoDistance.parse("2m")));

        Condition condition = new GeoShapeCondition(0.1f, "geo_point", WKT, GeoOperation.IS_WITHIN, transformations);

        Query query = condition.doQuery(schema);
        assertNotNull("Query is not built", query);

        assertEquals("Query type is wrong", WithinPrefixTreeQuery.class, query.getClass());
        assertEquals("Query is wrong",
                     "WithinPrefixTreeQuery(fieldName=geo_point.dist,queryShape=MULTIPOLYGON (((0.9999820135926447 1, " +
                     "0.9999820135926447 5, 0.9999823591964186 5.000003508974003, 0.9999833827263811 5.000006883100102, " +
                     "0.9999850448488496 5.000009992712526, 0.9999872816893899 5.00001271831061, 0.9999900072874744 5.00001495515115, " +
                     "0.9999931168998973 5.000016617273619, 0.9999964910259972 5.0000176408035815, 1 5.0000179864073555, " +
                     "5 5.0000179864073555, 5.000003508974003 5.0000176408035815, 5.000006883100102 5.000016617273619, " +
                     "5.000009992712526 5.00001495515115, 5.00001271831061 5.00001271831061, 5.00001495515115 5.000009992712526, " +
                     "5.000016617273619 5.000006883100102, 5.0000176408035815 5.000003508974003, 5.0000179864073555 5, " +
                     "5.0000179864073555 1, 5.0000176408035815 0.9999964910259972, 5.000016617273619 0.9999931168998973, " +
                     "5.00001495515115 0.9999900072874744, 5.00001271831061 0.9999872816893899, 5.000009992712526 0.9999850448488496, " +
                     "5.000006883100102 0.9999833827263811, 5.000003508974003 0.9999823591964186, 5 0.9999820135926447, " +
                     "1 0.9999820135926447, 0.9999964910259972 0.9999823591964186, 0.9999931168998973 0.9999833827263811, " +
                     "0.9999900072874744 0.9999850448488496, 0.9999872816893899 0.9999872816893899, 0.9999850448488496 " +
                     "0.9999900072874744, 0.9999833827263811 0.9999931168998973, 0.9999823591964186 0.9999964910259972, 0.9999820135926447 1), " +
                     "(0.9999910067963224 1, 0.9999911795982094 0.9999982455129985, 0.9999916913631905 0.9999965584499487, " +
                     "0.9999925224244248 0.9999950036437373, 0.999993640844695 0.999993640844695, 0.9999950036437373 0.9999925224244248, " +
                     "0.9999965584499487 0.9999916913631905, 0.9999982455129985 0.9999911795982094, 1 0.9999910067963224, 5 0.9999910067963224, " +
                     "5.000001754487001 0.9999911795982094, 5.000003441550051 0.9999916913631905, 5.000004996356263 0.9999925224244248, " +
                     "5.0000063591553054 0.999993640844695, 5.000007477575576 0.9999950036437373, 5.00000830863681 0.9999965584499487, " +
                     "5.00000882040179 0.9999982455129985, 5.000008993203678 1, 5.000008993203678 5, 5.00000882040179 5.000001754487001, " +
                     "5.00000830863681 5.000003441550051, 5.000007477575576 5.000004996356263, 5.0000063591553054 5.0000063591553054, " +
                     "5.000004996356263 5.000007477575576, 5.000003441550051 5.00000830863681, 5.000001754487001 5.00000882040179, " +
                     "5 5.000008993203678, 1 5.000008993203678, 0.9999982455129985 5.00000882040179, 0.9999965584499487 5.00000830863681, " +
                     "0.9999950036437373 5.000007477575576, 0.999993640844695 5.0000063591553054, 0.9999925224244248 5.000004996356263, " +
                     "0.9999916913631905 5.000003441550051, 0.9999911795982094 5.000001754487001, 0.9999910067963224 5, 0.9999910067963224 1)), " +
                     "((2.0000089932036778 2.0000089932036778, 2.0000089932036778 2.9999910067963222, 2.9999910067963222 2.9999910067963222, " +
                     "2.9999910067963222 2.0000089932036778, 2.0000089932036778 2.0000089932036778), (2.000017986407355 2.000017986407355, " +
                     "2.999982013592645 2.000017986407355, 2.999982013592645 2.999982013592645, 2.000017986407355 2.999982013592645, " +
                     "2.000017986407355 2.000017986407355))),detailLevel=8,prefixGridScanLevel=4)",
                     query.toString());
    }

    @Test
    public void testQueryIntersects() {
        Schema schema = schema().mapper("geo_point", geoPointMapper("lat", "lon").maxLevels(8)).build();

        List<GeoTransformation> transformations = new ArrayList<>();
        transformations.add(new GeoTransformation.Buffer(GeoDistance.parse("1m"), GeoDistance.parse("2m")));

        Condition condition = new GeoShapeCondition(0.1f, "geo_point", WKT, GeoOperation.INTERSECTS, transformations);

        Query query = condition.doQuery(schema);
        assertNotNull("Query is not built", query);

        assertEquals("Query type is wrong", IntersectsPrefixTreeQuery.class, query.getClass());
        assertEquals("Query is wrong",
                     "IntersectsPrefixTreeQuery(fieldName=geo_point.dist,queryShape=MULTIPOLYGON (((0.9999820135926447 1, 0.9999820135926447 5, " +
                     "0.9999823591964186 5.000003508974003, 0.9999833827263811 5.000006883100102, 0.9999850448488496 5.000009992712526, " +
                     "0.9999872816893899 5.00001271831061, 0.9999900072874744 5.00001495515115, 0.9999931168998973 5.000016617273619, " +
                     "0.9999964910259972 5.0000176408035815, 1 5.0000179864073555, 5 5.0000179864073555, 5.000003508974003 5.0000176408035815, " +
                     "5.000006883100102 5.000016617273619, 5.000009992712526 5.00001495515115, 5.00001271831061 5.00001271831061, " +
                     "5.00001495515115 5.000009992712526, 5.000016617273619 5.000006883100102, 5.0000176408035815 5.000003508974003, " +
                     "5.0000179864073555 5, 5.0000179864073555 1, 5.0000176408035815 0.9999964910259972, 5.000016617273619 0.9999931168998973, " +
                     "5.00001495515115 0.9999900072874744, 5.00001271831061 0.9999872816893899, 5.000009992712526 0.9999850448488496, " +
                     "5.000006883100102 0.9999833827263811, 5.000003508974003 0.9999823591964186, 5 0.9999820135926447, 1 0.9999820135926447, " +
                     "0.9999964910259972 0.9999823591964186, 0.9999931168998973 0.9999833827263811, 0.9999900072874744 0.9999850448488496, " +
                     "0.9999872816893899 0.9999872816893899, 0.9999850448488496 0.9999900072874744, 0.9999833827263811 0.9999931168998973, " +
                     "0.9999823591964186 0.9999964910259972, 0.9999820135926447 1), (0.9999910067963224 1, 0.9999911795982094 0.9999982455129985, " +
                     "0.9999916913631905 0.9999965584499487, 0.9999925224244248 0.9999950036437373, 0.999993640844695 0.999993640844695, " +
                     "0.9999950036437373 0.9999925224244248, 0.9999965584499487 0.9999916913631905, 0.9999982455129985 0.9999911795982094, " +
                     "1 0.9999910067963224, 5 0.9999910067963224, 5.000001754487001 0.9999911795982094, 5.000003441550051 0.9999916913631905, " +
                     "5.000004996356263 0.9999925224244248, 5.0000063591553054 0.999993640844695, 5.000007477575576 0.9999950036437373, " +
                     "5.00000830863681 0.9999965584499487, 5.00000882040179 0.9999982455129985, 5.000008993203678 1, 5.000008993203678 5, " +
                     "5.00000882040179 5.000001754487001, 5.00000830863681 5.000003441550051, 5.000007477575576 5.000004996356263, " +
                     "5.0000063591553054 5.0000063591553054, 5.000004996356263 5.000007477575576, 5.000003441550051 5.00000830863681, " +
                     "5.000001754487001 5.00000882040179, 5 5.000008993203678, 1 5.000008993203678, 0.9999982455129985 5.00000882040179, " +
                     "0.9999965584499487 5.00000830863681, 0.9999950036437373 5.000007477575576, 0.999993640844695 5.0000063591553054, " +
                     "0.9999925224244248 5.000004996356263, 0.9999916913631905 5.000003441550051, 0.9999911795982094 5.000001754487001, " +
                     "0.9999910067963224 5, 0.9999910067963224 1)), ((2.0000089932036778 2.0000089932036778, 2.0000089932036778 2.9999910067963222, " +
                     "2.9999910067963222 2.9999910067963222, 2.9999910067963222 2.0000089932036778, 2.0000089932036778 2.0000089932036778), " +
                     "(2.000017986407355 2.000017986407355, 2.999982013592645 2.000017986407355, 2.999982013592645 2.999982013592645, " +
                     "2.000017986407355 2.999982013592645, 2.000017986407355 2.000017986407355))),detailLevel=8,prefixGridScanLevel=4)",
                     query.toString());

    }

    @Test
    public void testQueryContains() {
        Schema schema = schema().mapper("geo_point", geoPointMapper("lat", "lon").maxLevels(8)).build();

        List<GeoTransformation> transformations = new ArrayList<>();
        transformations.add(new GeoTransformation.Buffer(GeoDistance.parse("1m"), GeoDistance.parse("2m")));

        Condition condition = new GeoShapeCondition(0.1f, "geo_point", WKT, GeoOperation.CONTAINS, transformations);

        Query query = condition.doQuery(schema);
        assertNotNull("Query is not built", query);

        assertEquals("Query type is wrong", ContainsPrefixTreeQuery.class, query.getClass());
        assertEquals("Query is wrong",
                     "ContainsPrefixTreeQuery(fieldName=geo_point.dist,queryShape=MULTIPOLYGON (((0.9999820135926447 1, 0.9999820135926447 5, " +
                     "0.9999823591964186 5.000003508974003, 0.9999833827263811 5.000006883100102, 0.9999850448488496 5.000009992712526, " +
                     "0.9999872816893899 5.00001271831061, 0.9999900072874744 5.00001495515115, 0.9999931168998973 5.000016617273619, " +
                     "0.9999964910259972 5.0000176408035815, 1 5.0000179864073555, 5 5.0000179864073555, 5.000003508974003 5.0000176408035815, " +
                     "5.000006883100102 5.000016617273619, 5.000009992712526 5.00001495515115, 5.00001271831061 5.00001271831061, " +
                     "5.00001495515115 5.000009992712526, 5.000016617273619 5.000006883100102, 5.0000176408035815 5.000003508974003, " +
                     "5.0000179864073555 5, 5.0000179864073555 1, 5.0000176408035815 0.9999964910259972, 5.000016617273619 0.9999931168998973, " +
                     "5.00001495515115 0.9999900072874744, 5.00001271831061 0.9999872816893899, 5.000009992712526 0.9999850448488496, " +
                     "5.000006883100102 0.9999833827263811, 5.000003508974003 0.9999823591964186, 5 0.9999820135926447, 1 0.9999820135926447, " +
                     "0.9999964910259972 0.9999823591964186, 0.9999931168998973 0.9999833827263811, 0.9999900072874744 0.9999850448488496, " +
                     "0.9999872816893899 0.9999872816893899, 0.9999850448488496 0.9999900072874744, 0.9999833827263811 0.9999931168998973, " +
                     "0.9999823591964186 0.9999964910259972, 0.9999820135926447 1), (0.9999910067963224 1, 0.9999911795982094 0.9999982455129985, " +
                     "0.9999916913631905 0.9999965584499487, 0.9999925224244248 0.9999950036437373, 0.999993640844695 0.999993640844695, " +
                     "0.9999950036437373 0.9999925224244248, 0.9999965584499487 0.9999916913631905, 0.9999982455129985 0.9999911795982094, " +
                     "1 0.9999910067963224, 5 0.9999910067963224, 5.000001754487001 0.9999911795982094, 5.000003441550051 0.9999916913631905, " +
                     "5.000004996356263 0.9999925224244248, 5.0000063591553054 0.999993640844695, 5.000007477575576 0.9999950036437373, " +
                     "5.00000830863681 0.9999965584499487, 5.00000882040179 0.9999982455129985, 5.000008993203678 1, 5.000008993203678 5, " +
                     "5.00000882040179 5.000001754487001, 5.00000830863681 5.000003441550051, 5.000007477575576 5.000004996356263, " +
                     "5.0000063591553054 5.0000063591553054, 5.000004996356263 5.000007477575576, 5.000003441550051 5.00000830863681, " +
                     "5.000001754487001 5.00000882040179, 5 5.000008993203678, 1 5.000008993203678, 0.9999982455129985 5.00000882040179, " +
                     "0.9999965584499487 5.00000830863681, 0.9999950036437373 5.000007477575576, 0.999993640844695 5.0000063591553054, " +
                     "0.9999925224244248 5.000004996356263, 0.9999916913631905 5.000003441550051, 0.9999911795982094 5.000001754487001, " +
                     "0.9999910067963224 5, 0.9999910067963224 1)), ((2.0000089932036778 2.0000089932036778, 2.0000089932036778 2.9999910067963222, " +
                     "2.9999910067963222 2.9999910067963222, 2.9999910067963222 2.0000089932036778, 2.0000089932036778 2.0000089932036778), " +
                     "(2.000017986407355 2.000017986407355, 2.999982013592645 2.000017986407355, 2.999982013592645 2.999982013592645, " +
                     "2.000017986407355 2.999982013592645, 2.000017986407355 2.000017986407355))),detailLevel=8,multiOverlappingIndexedShapes=true)",
                     query.toString());

    }

    @Test(expected = IndexException.class)
    public void testQueryWithoutValidMapper() {
        Schema schema = schema().mapper("name", uuidMapper()).build();
        String wkt = "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))";
        GeoShapeCondition condition = new GeoShapeCondition(0.1f, "geo_point", wkt, GeoOperation.CONTAINS, null);
        condition.query(schema);
    }

    @Test
    public void testToString() {
        String wkt = "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))";
        GeoShapeCondition condition = geoShape("name", wkt).operation("intersects").transformations(null).build();
        assertEquals("Method #toString is wrong", "GeoShapeCondition{boost=null, field=name, geometry=" +
                                                  "POLYGON ((1 1, 5 1, 5 5, 1 5, 1 1), (2 2, 3 2, 3 3, 2 3, 2 2)), " +
                                                  "operation=INTERSECTS, transformations=[]}", condition.toString());
    }
}
