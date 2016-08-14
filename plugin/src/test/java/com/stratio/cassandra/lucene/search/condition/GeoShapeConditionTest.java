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

import  org.locationtech.spatial4j.shape.jts.JtsGeometry;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.common.GeoDistance;
import com.stratio.cassandra.lucene.common.GeoOperation;
import com.stratio.cassandra.lucene.common.GeoTransformation;
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.composite.CompositeVerifyQuery;
import org.apache.lucene.spatial.composite.IntersectsRPTVerifyQuery;
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

        assertEquals("Query type is wrong", CompositeVerifyQuery.class, query.getClass());
        assertTrue("Query is wrong",
                   query.toString()
                        .startsWith("CompositeVerifyQuery(IntersectsPrefixTreeQuery(fieldName=geo_point,queryShape="));
    }

    @Test
    public void testQueryIntersects() {
        Schema schema = schema().mapper("geo_point", geoPointMapper("lat", "lon").maxLevels(8)).build();

        List<GeoTransformation> transformations = new ArrayList<>();
        transformations.add(new GeoTransformation.Buffer(GeoDistance.parse("1m"), GeoDistance.parse("2m")));

        Condition condition = new GeoShapeCondition(0.1f, "geo_point", WKT, GeoOperation.INTERSECTS, transformations);

        Query query = condition.doQuery(schema);
        assertNotNull("Query is not built", query);

        assertEquals("Query type is wrong", IntersectsRPTVerifyQuery.class, query.getClass());
        assertEquals("Query is wrong", "IntersectsVerified(fieldName=)", query.toString());

    }

    @Test
    public void testQueryContains() {
        Schema schema = schema().mapper("geo_point", geoPointMapper("lat", "lon").maxLevels(8)).build();

        List<GeoTransformation> transformations = new ArrayList<>();
        transformations.add(new GeoTransformation.Buffer(GeoDistance.parse("1m"), GeoDistance.parse("2m")));

        Condition condition = new GeoShapeCondition(0.1f, "geo_point", WKT, GeoOperation.CONTAINS, transformations);

        Query query = condition.doQuery(schema);
        assertNotNull("Query is not built", query);

        assertEquals("Query type is wrong", CompositeVerifyQuery.class, query.getClass());
        assertTrue("Query is wrong",
                   query.toString()
                        .startsWith("CompositeVerifyQuery(ContainsPrefixTreeQuery(fieldName=geo_point,queryShape="));

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
