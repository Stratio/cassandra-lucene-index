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
package com.stratio.cassandra.lucene.util;

import com.spatial4j.core.shape.jts.JtsGeometry;
import org.junit.Test;

import static com.stratio.cassandra.lucene.util.GeospatialUtilsJTS.geometry;
import static org.junit.Assert.assertEquals;

/**
 * Class for testing {@link GeospatialUtilsJTS}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeospatialUtilsJTSTest {

    @Test
    public void testParseWKTPoint() throws Exception {
        String wkt = "POINT (30.01 -10.7)";
        JtsGeometry geometry = geometry(wkt);
        assertEquals("Unable to parse WKT point", wkt, geometry.toString());
    }

    @Test
    public void testParseWKTLine() throws Exception {
        String wkt = "LINESTRING (30 10, 10 30, 40 40)";
        JtsGeometry geometry = geometry(wkt);
        assertEquals("Unable to parse WKT line", wkt, geometry.toString());
    }

    @Test
    public void testParseWKTPolygon() throws Exception {
        String wkt = "LINESTRING (30 10, 10 30, 40 40)";
        JtsGeometry geometry = geometry(wkt);
        assertEquals("Unable to parse WKT polygon", wkt, geometry.toString());
    }

    @Test
    public void testParseWKTMultipoint() throws Exception {
        String wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))";
        JtsGeometry geometry = geometry(wkt);
        assertEquals("Unable to parse WKT multipoint", wkt, geometry.toString());
    }

    @Test
    public void testParseWKTMultiline() throws Exception {
        String wkt = "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))";
        JtsGeometry geometry = geometry(wkt);
        assertEquals("Unable to parse WKT multiline", wkt, geometry.toString());
    }

    @Test
    public void testParseWKTMultipolygon() throws Exception {
        String wkt = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))";
        JtsGeometry geometry = geometry(wkt);
        assertEquals("Unable to parse WKT multipolygon", wkt, geometry.toString());
    }

    @Test
    public void testParseWKTSelfIntersectingPolygon() throws Exception {
        String wkt = "POLYGON((5 0, 10 0, 10 10, 0 10, 0 0, 5 0, 3 3, 5 6, 7 3, 5 0))";
        JtsGeometry geometry = geometry(wkt);
        assertEquals("Unable to parse invalid WKT shape",
                     "POLYGON ((5 0, 0 0, 0 10, 10 10, 10 0, 5 0), (5 0, 7 3, 5 6, 3 3, 5 0))",
                     geometry.toString());
    }
}
