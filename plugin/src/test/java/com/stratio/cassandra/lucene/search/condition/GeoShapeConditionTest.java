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

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.common.GeoOperation;
import com.stratio.cassandra.lucene.common.GeoShape;
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.composite.CompositeVerifyQuery;
import org.apache.lucene.spatial.composite.IntersectsRPTVerifyQuery;
import org.junit.Test;

import java.text.ParseException;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static com.stratio.cassandra.lucene.search.SearchBuilders.geoShape;
import static com.stratio.cassandra.lucene.common.GeospatialUtilsJTS.geometry;
import static org.junit.Assert.*;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoShapeConditionTest extends AbstractConditionTest {

    private static final String WKT = "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))";
    private static final GeoShape SHAPE = new GeoShape.WKT(WKT);

    @Test
    public void testConstructor() throws ParseException {
        GeoShapeCondition condition = new GeoShapeCondition(0.1f, "geo_point", SHAPE, GeoOperation.IS_WITHIN);

        assertEquals("Boost is not set", 0.1f, condition.boost, 0);
        assertEquals("Field is not set", "geo_point", condition.field);
        assertEquals("Geometry is not set", geometry(WKT), condition.shape.apply());
        assertEquals("Operation is not set", GeoOperation.IS_WITHIN, condition.operation);
    }

    @Test
    public void testConstructorWithDefaults() throws ParseException {
        GeoShapeCondition condition = new GeoShapeCondition(null, "geo_point", SHAPE, null);

        assertNull("Boost is not set to default", condition.boost);
        assertEquals("Field is not set", "geo_point", condition.field);
        assertEquals("Geometry is not set", geometry(WKT), condition.shape.apply());
        assertEquals("Operation is not set", GeoShapeCondition.DEFAULT_OPERATION, condition.operation);

    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullField() {
        new GeoShapeCondition(null, null, SHAPE, null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyField() {
        new GeoShapeCondition(null, "", SHAPE, null);

    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankField() {
        new GeoShapeCondition(null, " ", SHAPE, null);

    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullShape() {
        new GeoShapeCondition(null, "geo_point", null, null);
    }

    @Test(expected = IndexException.class)
    public void testQueryWithInvalidGeometry() {
        Schema schema = schema().mapper("geo_point", geoPointMapper("lat", "lon").maxLevels(8)).build();
        GeoShape shape = new GeoShape.WKT("POLYGONS((1 1,5 1,5 5,1 5,1 1))");
        Condition condition = new GeoShapeCondition(null, "geo_point", shape, null);
        condition.doQuery(schema);
    }

    @Test
    public void testQueryIsWithIn() {
        Schema schema = schema().mapper("geo_point", geoPointMapper("lat", "lon").maxLevels(8)).build();

        Condition condition = new GeoShapeCondition(0.1f, "geo_point", SHAPE, GeoOperation.IS_WITHIN);

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

        Condition condition = new GeoShapeCondition(0.1f, "geo_point", SHAPE, GeoOperation.INTERSECTS);

        Query query = condition.doQuery(schema);
        assertNotNull("Query is not built", query);

        assertEquals("Query type is wrong", IntersectsRPTVerifyQuery.class, query.getClass());
        assertEquals("Query is wrong", "IntersectsVerified(fieldName=)", query.toString());

    }

    @Test
    public void testQueryContains() {
        Schema schema = schema().mapper("geo_point", geoPointMapper("lat", "lon").maxLevels(8)).build();

        Condition condition = new GeoShapeCondition(0.1f, "geo_point", SHAPE, GeoOperation.CONTAINS);

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
        GeoShapeCondition condition = new GeoShapeCondition(0.1f, "geo_point", SHAPE, GeoOperation.CONTAINS);
        condition.query(schema);
    }

    @Test
    public void testToString() {
        String wkt = "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))";
        GeoShapeCondition condition = geoShape("name", SHAPE).operation(GeoOperation.INTERSECTS).build();
        assertEquals("Method #toString is wrong",
                     "GeoShapeCondition{boost=null, field=name, shape=WKT{" +
                     "value=POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))}, operation=INTERSECTS}",
                     condition.toString());
    }
}
