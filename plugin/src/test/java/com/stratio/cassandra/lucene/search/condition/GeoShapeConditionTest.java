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

package com.stratio.cassandra.lucene.search.condition;

import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.schema.Schema;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.ContainsPrefixTreeFilter;
import org.apache.lucene.spatial.prefix.IntersectsPrefixTreeFilter;
import org.apache.lucene.spatial.prefix.WithinPrefixTreeFilter;
import org.junit.Test;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import static com.stratio.cassandra.lucene.schema.SchemaBuilders.*;
import static com.stratio.cassandra.lucene.search.SearchBuilders.geoShape;
import static org.junit.Assert.*;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoShapeConditionTest extends AbstractConditionTest {

    @Test
    public void testConstructor() throws ParseException {
        List<GeoTransformation> transformationList = new ArrayList<>();

        transformationList.add(new GeoTransformation.Copy());
        transformationList.add(new GeoTransformation.Buffer(GeoDistance.parse("2m"), GeoDistance.parse("1m")));

        GeoShapeCondition condition = new GeoShapeCondition(0.1f,
                                                            "geo_point",
                                                            "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))",
                                                            GeoOperation.IS_WITHIN,
                                                            transformationList);

        Shape shape = GeoShapeCondition.CONTEXT.getWktShapeParser()
                                               .parse("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry geo = GeoShapeCondition.CONTEXT.makeShape(GeoShapeCondition.CONTEXT.getGeometryFrom(shape));
        assertEquals("Boost is not set", 0.1f, condition.boost, 0);
        assertEquals("Field is not set", "geo_point", condition.field);
        assertEquals("Geometry is not set", geo, condition.geometry);
        assertEquals("Operation is not set", GeoOperation.IS_WITHIN, condition.operation);
        assertEquals("Transformations is not set", transformationList, condition.transformations);
    }

    @Test
    public void testConstructorWithDefaults() throws ParseException {
        GeoShapeCondition condition = new GeoShapeCondition(null,
                                                            "geo_point",
                                                            "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))",
                                                            null,
                                                            null);

        assertEquals("Boost is not to default", Condition.DEFAULT_BOOST, condition.boost, 0);
        assertEquals("Field is not set", "geo_point", condition.field);

        Shape shape = GeoShapeCondition.CONTEXT.getWktShapeParser()
                                               .parse("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry geo = GeoShapeCondition.CONTEXT.makeShape(GeoShapeCondition.CONTEXT.getGeometryFrom(shape));

        assertEquals("Geometry is not set", geo, condition.geometry);
        assertEquals("Operation is not set", GeoShapeCondition.DEFAULT_OPERATION, condition.operation);
        assertEquals("Transformations is not set", new ArrayList(), condition.transformations);

    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullField() {
        new GeoShapeCondition(null, null, "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))", null, null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithEmptyField() {
        new GeoShapeCondition(null, "", "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))", null, null);

    }

    @Test(expected = IndexException.class)
    public void testConstructorWithBlankField() {
        new GeoShapeCondition(null, " ", "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))", null, null);

    }

    @Test(expected = IndexException.class)
    public void testConstructorWithNullGeometry() {
        new GeoShapeCondition(null, "geo_point", null, null, null);
    }

    @Test(expected = IndexException.class)
    public void testConstructorWithInvalidGeometry() {
        new GeoShapeCondition(null,
                              "geo_point",
                              "POLYGONS((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))",
                              null,
                              null);

    }

    @Test(expected = IndexException.class)
    public void testConstructorWithInvalidGeometry2() {
        new GeoShapeCondition(null,
                              "geo_point",
                              "POLYGON((1.7 1,5 1,5 5,1 5,1.7 2),(2 2, 3 2, 3 3, 2 3,2 2))",
                              null,
                              null);

    }

    @Test
    public void testQueryIsWithIn() {
        Schema schema = schema().mapper("geo_point", geoPointMapper("lat", "lon").maxLevels(8)).build();

        List<GeoTransformation> transformationList = new ArrayList<>();
        transformationList.add(new GeoTransformation.Copy());
        transformationList.add(new GeoTransformation.Buffer(GeoDistance.parse("2m"), GeoDistance.parse("1m")));

        GeoShapeCondition condition = new GeoShapeCondition(0.1f,
                                                            "geo_point",
                                                            "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))",
                                                            GeoOperation.IS_WITHIN,
                                                            transformationList);

        Query query = condition.query(schema);
        assertNotNull("Query is not built", query);

        assertTrue("Query type is wrong", query instanceof ConstantScoreQuery);
        query = ((ConstantScoreQuery) query).getQuery();
        assertTrue("Query type is wrong", query instanceof WithinPrefixTreeFilter);
        WithinPrefixTreeFilter withinPrefixTreeFilter = (WithinPrefixTreeFilter) query;
        assertEquals("Query is wrong",
                     "WithinPrefixTreeFilter(fieldName=geo_point.dist,queryShape=POLYGON ((1 1, 5 1, 5 5, 1 5, 1 1)," +
                     " (2 2, 3 2, 3 3, 2 3, 2 2)),detailLevel=8,prefixGridScanLevel=4)",
                     withinPrefixTreeFilter.toString());
    }

    @Test
    public void testQueryIntersects() {
        Schema schema = schema().mapper("geo_point", geoPointMapper("lat", "lon").maxLevels(8)).build();

        List<GeoTransformation> transformationList = new ArrayList<>();
        transformationList.add(new GeoTransformation.Copy());
        transformationList.add(new GeoTransformation.Buffer(GeoDistance.parse("2m"), GeoDistance.parse("1m")));

        GeoShapeCondition condition = new GeoShapeCondition(0.1f,
                                                            "geo_point",
                                                            "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))",
                                                            GeoOperation.INTERSECTS,
                                                            transformationList);

        Query query = condition.query(schema);
        assertNotNull("Query is not built", query);

        assertTrue("Query type is wrong", query instanceof ConstantScoreQuery);
        query = ((ConstantScoreQuery) query).getQuery();
        assertTrue("Query type is wrong", query instanceof IntersectsPrefixTreeFilter);
        IntersectsPrefixTreeFilter intersectsPrefixTreeFilter = (IntersectsPrefixTreeFilter) query;
        assertEquals("Query is wrong",
                     "IntersectsPrefixTreeFilter(fieldName=geo_point.dist,queryShape=POLYGON ((1 1, 5 1, 5 5, 1 5, 1 1)," +
                     " (2 2, 3 2, 3 3, 2 3, 2 2)),detailLevel=8,prefixGridScanLevel=4)",
                     intersectsPrefixTreeFilter.toString());

    }

    @Test
    public void testQueryContains() {
        Schema schema = schema().mapper("geo_point", geoPointMapper("lat", "lon").maxLevels(8)).build();

        List<GeoTransformation> transformationList = new ArrayList<>();
        transformationList.add(new GeoTransformation.Copy());
        transformationList.add(new GeoTransformation.Buffer(GeoDistance.parse("2m"), GeoDistance.parse("1m")));

        GeoShapeCondition condition = new GeoShapeCondition(0.1f,
                                                            "geo_point",
                                                            "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))",
                                                            GeoOperation.CONTAINS,
                                                            transformationList);

        Query query = condition.query(schema);
        assertNotNull("Query is not built", query);

        assertTrue("Query type is wrong", query instanceof ConstantScoreQuery);
        query = ((ConstantScoreQuery) query).getQuery();
        assertTrue("Query type is wrong", query instanceof ContainsPrefixTreeFilter);
        ContainsPrefixTreeFilter containsPrefixTreeFilter = (ContainsPrefixTreeFilter) query;
        assertEquals("Query is wrong",
                     "ContainsPrefixTreeFilter(fieldName=geo_point.dist,queryShape=POLYGON ((1 1, 5 1, 5 5, 1 5, 1 1)," +
                     " (2 2, 3 2, 3 3, 2 3, 2 2)),detailLevel=8,multiOverlappingIndexedShapes=true)",
                     containsPrefixTreeFilter.toString());

    }

    @Test(expected = IndexException.class)
    public void testQueryWithoutValidMapper() {
        Schema schema = schema().mapper("name", uuidMapper()).build();
        GeoShapeCondition condition = new GeoShapeCondition(0.1f,
                                                            "geo_point",
                                                            "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))",
                                                            GeoOperation.CONTAINS,
                                                            null);
        condition.query(schema);
    }

    @Test
    public void testToString() {
        GeoShapeCondition condition = geoShape("name",
                                               "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))").setOperation(
                "intersects").setTransformations(null).build();
        assertEquals("Method #toString is wrong",
                     "GeoShapeCondition{boost=1.0, field=name, geometry=POLYGON ((1 1, 5 1, 5 5, 1 5, 1 1), " +
                     "(2 2, 3 2, 3 3, 2 3, 2 2)), operation=INTERSECTS, transformations=[]}",
                     condition.toString());
    }
}
