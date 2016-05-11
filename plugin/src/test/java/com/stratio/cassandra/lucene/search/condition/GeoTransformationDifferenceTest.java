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

import static com.stratio.cassandra.lucene.common.GeoTransformation.Difference;
import static com.stratio.cassandra.lucene.util.GeospatialUtilsJTS.geometry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for {@link GeoTransformation.Difference}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoTransformationDifferenceTest extends AbstractConditionTest {

    @Test
    public void testDifferenceTransformation() {

        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        String shape2 = "POLYGON((-20 20, -20 0, 20 0, 20 20, -20 20))";
        String difference = "POLYGON ((-20 0, -30 0, -30 30, 30 30, 30 0, 20 0, 20 20, -20 20, -20 0))";

        GeoTransformation transformation = new Difference(shape2);
        JtsGeometry geometry = geometry(shape1);
        JtsGeometry transformedGeometry = transformation.apply(geometry);

        assertEquals("Failed applied DifferenceTransformation", difference, transformedGeometry.toString());
    }

    @Test(expected = IndexException.class)
    public void testDifferenceTransformationWithNullShape() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new Difference(null);
        JtsGeometry geometry = geometry(shape1);
        transformation.apply(geometry);
    }

    @Test(expected = IndexException.class)
    public void testDifferenceTransformationWithEmptyShape() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new Difference("");
        JtsGeometry geometry = geometry(shape1);
        transformation.apply(geometry);
    }

    @Test(expected = IndexException.class)
    public void testDifferenceTransformationWithWrongShape() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new Difference("POLYGON((-30 0, 30 0, 30 -30, -30 -30))");
        JtsGeometry geometry = geometry(shape1);
        transformation.apply(geometry);
    }

    @Test
    public void testDifferenceTransformationToString() {
        String shape1 = "POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))";
        GeoTransformation transformation = new Difference(shape1);
        assertEquals("Difference{other=POLYGON((-30 30, -30 0, 30 0, 30 30, -30 30))}", transformation.toString());
    }

    @Test
    public void testDifferenceTransformationParsing() throws IOException {
        String json = "{type:\"difference\",shape:\"LINESTRING(2 28, 30 3)\"}";
        Difference difference = JsonSerializer.fromString(json, Difference.class);
        assertNotNull("JSON serialization is wrong", difference);
        assertEquals("JSON shape serialization is wrong", "LINESTRING(2 28, 30 3)", difference.other);
    }

}
