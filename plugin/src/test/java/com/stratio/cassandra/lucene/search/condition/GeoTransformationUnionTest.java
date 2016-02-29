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
 * Unit tests for {@link GeoTransformation.Union}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoTransformationUnionTest extends AbstractConditionTest {

    private static final JtsSpatialContext CONTEXT = GeoShapeMapper.SPATIAL_CONTEXT;

    private static JtsGeometry geometry(String string) {
        return GeospatialUtils.geometryFromWKT(CONTEXT, string);
    }

    @Test
    public void testUnionTransformation() {

        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        String shape2 = "POLYGON((-30 0, 30 0, 30 -30, -30 -30, -30 0))";
        String union = "POLYGON ((-30 0, -30 30, 30 30, 30 0, 30 -30, -30 -30, -30 0))";

        GeoTransformation transformation = new GeoTransformation.Union(shape2);
        JtsGeometry geometry = geometry(shape1);
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);

        assertEquals("Failed applied UnionTransformation", union, transformedGeometry.toString());
    }

    @Test(expected = IndexException.class)
    public void testUnionTransformationWithNullShape() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new GeoTransformation.Union(null);
        JtsGeometry geometry = geometry(shape1);
        transformation.apply(geometry, GeoShapeCondition.CONTEXT);
    }

    @Test(expected = IndexException.class)
    public void testUnionTransformationWithEmptyShape() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new GeoTransformation.Union("");
        JtsGeometry geometry = geometry(shape1);
        transformation.apply(geometry, GeoShapeCondition.CONTEXT);
    }

    @Test(expected = IndexException.class)
    public void testUnionTransformationWithWrongShape() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new GeoTransformation.Union("POLYGON((-30 0, 30 0, 30 -30, -30 -30))");
        JtsGeometry geometry = geometry(shape1);
        transformation.apply(geometry, GeoShapeCondition.CONTEXT);
    }

    @Test
    public void testUnionTransformationToString() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new GeoTransformation.Union(shape1);
        assertEquals("Union{other=POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))}", transformation.toString());
    }

    @Test
    public void testUnionTransformationBuilder() throws IOException {
        GeoTransformationBuilder builder = new GeoTransformationBuilder.Union("LINESTRING(2 4, 30 3)");
        String json = JsonSerializer.toString(builder);
        assertEquals("JSON serialization is wrong", "{type:\"union\",shape:\"LINESTRING(2 4, 30 3)\"}", json);
        builder = JsonSerializer.fromString(json, GeoTransformationBuilder.Union.class);
        assertEquals("JSON parsing is wrong ", "Union{other=LINESTRING(2 4, 30 3)}", builder.build().toString());
    }

}
