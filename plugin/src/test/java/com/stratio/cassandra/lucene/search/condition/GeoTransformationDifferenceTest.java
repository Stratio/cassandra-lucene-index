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
import com.stratio.cassandra.lucene.schema.mapping.GeoShapeMapper;
import com.stratio.cassandra.lucene.search.condition.builder.GeoTransformationBuilder;
import com.stratio.cassandra.lucene.util.GeospatialUtils;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

/**
 * Unit tests for {@link GeoTransformation.Difference}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoTransformationDifferenceTest extends AbstractConditionTest {

    private static final JtsSpatialContext CONTEXT = GeoShapeMapper.SPATIAL_CONTEXT;

    private static JtsGeometry geometry(String string) {
        return GeospatialUtils.geometryFromWKT(CONTEXT, string);
    }

    @Test
    public void testDifferenceTransformation() {

        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        String shape2 = "POLYGON((-20 20, -20 0, 20 0, 20 20, -20 20))";
        String difference = "POLYGON ((-20 0, -30 0, -30 30, 30 30, 30 0, 20 0, 20 20, -20 20, -20 0))";

        GeoTransformation transformation = new GeoTransformation.Difference(shape2);
        JtsGeometry geometry = geometry(shape1);
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);

        assertEquals("Failed applied DifferenceTransformation", difference, transformedGeometry.toString());
    }

    @Test(expected = IndexException.class)
    public void testDifferenceTransformationWithNullShape() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new GeoTransformation.Difference(null);
        JtsGeometry geometry = geometry(shape1);
        transformation.apply(geometry, GeoShapeCondition.CONTEXT);
    }

    @Test(expected = IndexException.class)
    public void testDifferenceTransformationWithEmptyShape() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new GeoTransformation.Difference("");
        JtsGeometry geometry = geometry(shape1);
        transformation.apply(geometry, GeoShapeCondition.CONTEXT);
    }

    @Test(expected = IndexException.class)
    public void testDifferenceTransformationWithWrongShape() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new GeoTransformation.Difference("POLYGON((-30 0, 30 0, 30 -30, -30 -30))");
        JtsGeometry geometry = geometry(shape1);
        transformation.apply(geometry, GeoShapeCondition.CONTEXT);
    }

    @Test
    public void testDifferenceTransformationToString() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new GeoTransformation.Difference(shape1);
        assertEquals("Difference{other=POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))}", transformation.toString());
    }

    @Test
    public void testDifferenceTransformationBuilder() throws IOException {
        GeoTransformationBuilder builder = new GeoTransformationBuilder.Difference("LINESTRING(2 28, 30 3)");
        String json = JsonSerializer.toString(builder);
        assertEquals("JSON serialization is wrong", "{type:\"difference\",shape:\"LINESTRING(2 28, 30 3)\"}", json);
        builder = JsonSerializer.fromString(json, GeoTransformationBuilder.Difference.class);
        assertEquals("JSON parsing is wrong ", "Difference{other=LINESTRING(2 28, 30 3)}", builder.build().toString());
    }

}
