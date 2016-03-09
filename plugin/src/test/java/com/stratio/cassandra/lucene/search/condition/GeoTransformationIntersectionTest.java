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

import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.common.GeoTransformation;
import com.stratio.cassandra.lucene.schema.mapping.GeoShapeMapper;
import com.stratio.cassandra.lucene.util.GeospatialUtilsJTS;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.junit.Test;

import java.io.IOException;

import static com.stratio.cassandra.lucene.common.GeoTransformation.Intersection;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * Unit tests for {@link GeoTransformation.Intersection}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoTransformationIntersectionTest extends AbstractConditionTest {

    private static final JtsSpatialContext CONTEXT = GeoShapeMapper.SPATIAL_CONTEXT;

    private static JtsGeometry geometry(String string) {
        return GeospatialUtilsJTS.geometryFromWKT(CONTEXT, string);
    }

    @Test
    public void testIntersectionTransformation() {

        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        String shape2 = "POLYGON((-30 0, 30 0, 30 -30, -30 -30, -30 0))";
        String union = "LINESTRING (-30 0, 30 0)";

        GeoTransformation transformation = new Intersection(shape2);
        JtsGeometry geometry = geometry(shape1);
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);

        assertEquals("Failed applied IntersectionTransformation", union, transformedGeometry.toString());
    }

    @Test(expected = IndexException.class)
    public void testIntersectionTransformationWithNullShape() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new Intersection(null);
        JtsGeometry geometry = geometry(shape1);
        transformation.apply(geometry, GeoShapeCondition.CONTEXT);
    }

    @Test(expected = IndexException.class)
    public void testIntersectionTransformationWithEmptyShape() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new Intersection("");
        JtsGeometry geometry = geometry(shape1);
        transformation.apply(geometry, GeoShapeCondition.CONTEXT);
    }

    @Test(expected = IndexException.class)
    public void testIntersectionTransformationWithWrongShape() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new Intersection("POLYGON((-30 0, 30 0, 30 -30, -30 -30))");
        JtsGeometry geometry = geometry(shape1);
        transformation.apply(geometry, GeoShapeCondition.CONTEXT);
    }

    @Test
    public void testIntersectionTransformationToString() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new GeoTransformation.Intersection(shape1);
        assertEquals("Intersection{other=POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))}", transformation.toString());
    }

    @Test
    public void testIntersectionTransformationParsing() throws IOException {
        String json = "{type:\"intersection\",shape:\"LINESTRING(2 4, 30 3)\"}";
        Intersection intersection = JsonSerializer.fromString(json, Intersection.class);
        assertNotNull("JSON serialization is wrong", intersection);
        assertEquals("JSON serialization is wrong", "LINESTRING(2 4, 30 3)", intersection.other);
    }

}
