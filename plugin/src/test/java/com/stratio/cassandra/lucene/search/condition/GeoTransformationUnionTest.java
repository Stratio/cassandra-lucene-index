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
import com.stratio.cassandra.lucene.common.GeoTransformation;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.junit.Test;

import java.io.IOException;

import static com.stratio.cassandra.lucene.common.GeoTransformation.Union;
import static com.stratio.cassandra.lucene.util.GeospatialUtilsJTS.geometry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for {@link GeoTransformation.Union}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoTransformationUnionTest extends AbstractConditionTest {

    @Test
    public void testUnionTransformation() {

        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        String shape2 = "POLYGON((-30 0, 30 0, 30 -30, -30 -30, -30 0))";
        String union = "POLYGON ((-30 0, -30 30, 30 30, 30 0, 30 -30, -30 -30, -30 0))";

        GeoTransformation transformation = new Union(shape2);
        JtsGeometry geometry = geometry(shape1);
        JtsGeometry transformedGeometry = transformation.apply(geometry);

        assertEquals("Failed applied UnionTransformation", union, transformedGeometry.toString());
    }

    @Test(expected = IndexException.class)
    public void testUnionTransformationWithNullShape() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new Union(null);
        JtsGeometry geometry = geometry(shape1);
        transformation.apply(geometry);
    }

    @Test(expected = IndexException.class)
    public void testUnionTransformationWithEmptyShape() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new Union("");
        JtsGeometry geometry = geometry(shape1);
        transformation.apply(geometry);
    }

    @Test(expected = IndexException.class)
    public void testUnionTransformationWithWrongShape() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new Union("POLYGON((-30 0, 30 0, 30 -30, -30 -30))");
        JtsGeometry geometry = geometry(shape1);
        transformation.apply(geometry);
    }

    @Test
    public void testUnionTransformationToString() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new Union(shape1);
        assertEquals("Union{other=POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))}", transformation.toString());
    }

    @Test
    public void testUnionTransformationParsing() throws IOException {
        String json = "{type:\"union\",shape:\"LINESTRING(2 4, 30 3)\"}";
        Union union = JsonSerializer.fromString(json, Union.class);
        assertNotNull("JSON serialization is wrong", union);
        assertEquals("JSON shape serialization is wrong", "LINESTRING(2 4, 30 3)", union.other);
    }

}
