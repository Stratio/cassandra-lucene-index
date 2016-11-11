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
package com.stratio.cassandra.lucene.common;

import org.junit.Test;

import java.io.IOException;

import static com.stratio.cassandra.lucene.common.GeoShape.*;
import static org.junit.Assert.assertEquals;

/**
 * Class for testing {@link GeoShape}s.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoShapeTest {

    private static GeoShape parse(String json) throws IOException {
        return JsonSerializer.fromString(json, GeoShape.class);
    }

    @Test
    public void testWKT() throws IOException {
        GeoShape shape = parse("{type:\"wkt\", value:\"POINT(0 0)\"}");
        assertEquals("Geo shape parsing is wrong", WKT.class, shape.getClass());
        WKT wkt = (WKT) shape;
        assertEquals("Geo shape parsing is wrong", "POINT(0 0)", wkt.value);
        assertEquals("Geo shape parsing is wrong", "WKT{value=POINT(0 0)}", wkt.toString());
        assertEquals("Geo shape calculation is wrong", "POINT (0 0)", wkt.apply().toString());
    }

    @Test
    public void testBBox() throws IOException {
        GeoShape shape = parse("{type:\"bbox\", shape:{type:\"wkt\", value:\"POLYGON((-1 1, 1 1, -1 -1, -1 1))\"}}");
        assertEquals("Geo shape parsing is wrong", BBox.class, shape.getClass());
        BBox bbox = (BBox) shape;
        assertEquals("BBox geo shape calculation is wrong",
                     "POLYGON ((-1 -1, -1 1, 1 1, 1 -1, -1 -1))",
                     bbox.apply().toString());
    }

    @Test
    public void testCentroid() throws IOException {
        GeoShape shape = parse("{type:\"centroid\", " +
                               "shape:{type:\"wkt\", value:\"POLYGON((-1 1, 1 1, 1 -1, -1 -1, -1 1))\"}}");
        assertEquals("Geo shape parsing is wrong", Centroid.class, shape.getClass());
        Centroid centroid = (Centroid) shape;
        assertEquals("Centroid geo shape calculation is wrong", "POINT (-0 -0)", centroid.apply().toString());
    }

    @Test
    public void testBuffer() throws IOException {
        GeoShape shape = parse("{type:\"buffer\", max_distance:\"1000km\", " +
                               "shape:{type:\"wkt\", value:\"POINT(0 0)\"}}");
        assertEquals("Buffer geo shape parsing is wrong", Buffer.class, shape.getClass());
        Buffer buffer = (Buffer) shape;
        assertEquals("Buffer geo shape calculation is wrong",
                     "POLYGON ((8.993203677616636 0, 8.820401790674595 -1.7544870014228584, " +
                     "8.308636809455242 -3.4415500513086825, 7.477575575185728 -4.996356262766218, " +
                     "6.3591553050345215 -6.359155305034521, 4.996356262766219 -7.477575575185728, " +
                     "3.441550051308683 -8.308636809455242, 1.754487001422859 -8.820401790674595, " +
                     "0.0000000000000006 -8.993203677616636, -1.7544870014228577 -8.820401790674595, " +
                     "-3.441550051308682 -8.308636809455242, -4.9963562627662155 -7.477575575185729, " +
                     "-6.359155305034521 -6.3591553050345215, -7.477575575185729 -4.996356262766218, " +
                     "-8.308636809455244 -3.4415500513086794, -8.820401790674596 -1.7544870014228535, " +
                     "-8.993203677616636 0.0000000000000069, -8.820401790674593 1.754487001422867, " +
                     "-8.308636809455239 3.4415500513086927, -7.477575575185721 4.996356262766229, " +
                     "-6.359155305034511 6.359155305034531, -4.996356262766205 7.477575575185737, " +
                     "-3.4415500513086656 8.30863680945525, -1.7544870014228386 8.8204017906746, " +
                     "0.0000000000000223 8.993203677616636, 1.7544870014228824 8.820401790674591, " +
                     "3.4415500513087065 8.308636809455232, 4.996356262766241 7.477575575185712, " +
                     "6.359155305034543 6.359155305034499, 7.477575575185746 4.9963562627661915, " +
                     "8.308636809455255 3.441550051308651, 8.820401790674602 1.7544870014228233, " +
                     "8.993203677616636 0))", buffer.apply().toString());
    }
}