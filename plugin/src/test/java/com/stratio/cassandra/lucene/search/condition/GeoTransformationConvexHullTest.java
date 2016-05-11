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
import com.stratio.cassandra.lucene.common.GeoTransformation;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.junit.Test;

import java.io.IOException;

import static com.stratio.cassandra.lucene.common.GeoTransformation.ConvexHull;
import static com.stratio.cassandra.lucene.util.GeospatialUtilsJTS.geometry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for {@link ConvexHull}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoTransformationConvexHullTest extends AbstractConditionTest {

    @Test
    public void testConvexHullTransformationPoint() {

        String shape = "POINT (-30.1 50.2)";
        String convexHull = "POINT (-30.1 50.2)";

        GeoTransformation transformation = new ConvexHull();
        JtsGeometry geometry = geometry(shape);
        JtsGeometry transformedGeometry = transformation.apply(geometry);

        assertEquals("Failed applied convex hull transformation to point", convexHull, transformedGeometry.toString());
    }

    @Test
    public void testConvexHullTransformationLine() {

        String shape = "LINESTRING (0 -30, 0 50)";
        String convexHull = "LINESTRING (0 -30, 0 50)";

        GeoTransformation transformation = new ConvexHull();
        JtsGeometry geometry = geometry(shape);
        JtsGeometry transformedGeometry = transformation.apply(geometry);

        assertEquals("Failed applied convex hull transformation to line", convexHull, transformedGeometry.toString());
    }

    @Test
    public void testConvexHullTransformationPolygon() {

        String shape = "POLYGON ((-30 30, -30 0, 30 0, 30 30, -30 30))";
        String convexHull = "POLYGON ((-30 0, -30 30, 30 30, 30 0, -30 0))";

        GeoTransformation transformation = new ConvexHull();
        JtsGeometry geometry = geometry(shape);
        JtsGeometry transformedGeometry = transformation.apply(geometry);

        assertEquals("Failed applied convex hull transformation to polygon",
                     convexHull,
                     transformedGeometry.toString());
    }

    @Test
    public void testConvexHullTransformationMultiPolygon() {

        String shape = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))";
        String convexHull = "POLYGON ((15 5, 5 10, 10 40, 45 40, 40 10, 15 5))";

        GeoTransformation transformation = new ConvexHull();
        JtsGeometry geometry = geometry(shape);
        JtsGeometry transformedGeometry = transformation.apply(geometry);

        assertEquals("Failed applied convex hull transformation to multipart",
                     convexHull,
                     transformedGeometry.toString());
    }

    @Test
    public void testConvexHullTransformationToString() {
        GeoTransformation transformation = new ConvexHull();
        assertEquals("ConvexHull{}", transformation.toString());
    }

    @Test
    public void testCConvexHullTransformationParsing() throws IOException {
        String json = "{type:\"convex_hull\"}";
        ConvexHull convexHull = JsonSerializer.fromString(json, ConvexHull.class);
        assertNotNull("JSON shape serialization is wrong", convexHull);
    }

}
