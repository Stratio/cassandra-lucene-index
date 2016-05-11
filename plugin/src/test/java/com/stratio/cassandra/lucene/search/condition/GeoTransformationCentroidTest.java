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

import static com.stratio.cassandra.lucene.common.GeoTransformation.Centroid;
import static com.stratio.cassandra.lucene.util.GeospatialUtilsJTS.geometry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for {@link GeoTransformation.Centroid}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoTransformationCentroidTest extends AbstractConditionTest {

    @Test
    public void testCentroidTransformationPoint() {

        String shape = "POINT (-30.1 50.2)";
        String centroid = "POINT (-30.1 50.2)";

        GeoTransformation transformation = new GeoTransformation.Centroid();
        JtsGeometry geometry = geometry(shape);
        JtsGeometry transformedGeometry = transformation.apply(geometry);

        assertEquals("Failed applied centroid transformation to point", centroid, transformedGeometry.toString());
    }

    @Test
    public void testCentroidTransformationLine() {

        String shape = "LINESTRING (0 -30, 0 50)";
        String centroid = "POINT (0 10)";

        GeoTransformation transformation = new GeoTransformation.Centroid();
        JtsGeometry geometry = geometry(shape);
        JtsGeometry transformedGeometry = transformation.apply(geometry);

        assertEquals("Failed applied centroid transformation to line", centroid, transformedGeometry.toString());
    }

    @Test
    public void testCentroidTransformationPolygon() {

        String shape = "POLYGON ((-30 30, -30 0, 30 0, 30 30, -30 30))";
        String centroid = "POINT (-0 15)";

        GeoTransformation transformation = new GeoTransformation.Centroid();
        JtsGeometry geometry = geometry(shape);
        JtsGeometry transformedGeometry = transformation.apply(geometry);

        assertEquals("Failed applied centroid transformation to polygon", centroid, transformedGeometry.toString());
    }

    @Test
    public void testCentroidTransformationMultiPolygon() {

        String shape = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))";
        String centroid = "POINT (24.285714285714285 24.047619047619047)";

        GeoTransformation transformation = new GeoTransformation.Centroid();
        JtsGeometry geometry = geometry(shape);
        JtsGeometry transformedGeometry = transformation.apply(geometry);

        assertEquals("Failed applied centroid transformation to multipart", centroid, transformedGeometry.toString());
    }

    @Test
    public void testCentroidTransformationToString() {
        GeoTransformation transformation = new GeoTransformation.Centroid();
        assertEquals("Centroid{}", transformation.toString());
    }

    @Test
    public void testCentroidTransformationParsing() throws IOException {
        String json = "{type:\"centroid\"}";
        Centroid centroid = JsonSerializer.fromString(json, Centroid.class);
        assertNotNull("JSON shape serialization is wrong", centroid);
    }

}
