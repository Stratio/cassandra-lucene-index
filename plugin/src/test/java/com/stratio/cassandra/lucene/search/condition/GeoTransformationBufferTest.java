/**
 * Copyright (C) 2016 Stratio (http://stratio.com)
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

import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.stratio.cassandra.lucene.common.GeoDistance;
import com.stratio.cassandra.lucene.common.GeoDistanceUnit;
import com.stratio.cassandra.lucene.common.GeoTransformation;
import com.stratio.cassandra.lucene.schema.mapping.GeoShapeMapper;
import com.stratio.cassandra.lucene.util.GeospatialUtilsJTS;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import com.vividsolutions.jts.geom.Geometry;
import org.junit.Test;

import java.io.IOException;

import static com.stratio.cassandra.lucene.common.GeoTransformation.Buffer;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * Unit tests for {@link GeoTransformation.Buffer}.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoTransformationBufferTest extends AbstractConditionTest {

    private static final JtsSpatialContext CONTEXT = GeoShapeMapper.SPATIAL_CONTEXT;

    private static JtsGeometry geometry(String string) {
        return GeospatialUtilsJTS.geometryFromWKT(CONTEXT, string);
    }

    @Test
    public void testBufferTransformationWithNullMaxDistance() {
        GeoDistance min = GeoDistance.parse("1m");

        GeoTransformation transformation = new Buffer().minDistance(min);
        JtsGeometry geometry = geometry("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);

        JtsGeometry max = GeoShapeCondition.CONTEXT.makeShape(geometry.getGeom());
        JtsGeometry minGeometry = geometry.getBuffered(min.getDegrees(), GeoShapeCondition.CONTEXT);
        Geometry difference = max.getGeom().difference(minGeometry.getGeom());
        JtsGeometry desiredGeometry = GeoShapeCondition.CONTEXT.makeShape(difference);

        assertEquals("Failed applied BufferTransformation", desiredGeometry, transformedGeometry);
    }

    @Test
    public void testBufferTransformationWithNullMinDistance() {
        GeoDistance max = GeoDistance.parse("2m");

        GeoTransformation transformation = new Buffer().maxDistance(max);
        JtsGeometry geometry = geometry("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);

        JtsGeometry desiredGeometry = geometry.getBuffered(max.getDegrees(), GeoShapeCondition.CONTEXT);
        assertEquals("Failed applied BufferTransformation", desiredGeometry, transformedGeometry);
    }

    @Test
    public void testBufferTransformationWithNullMinMaxDistance() {
        GeoTransformation transformation = new Buffer();
        JtsGeometry geometry = geometry("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);
        assertEquals("Applied BufferTransformation with min and max to null must return a equals geometry",
                     geometry,
                     transformedGeometry);
    }

    @Test
    public void testBufferTransformationWithPositiveDistances() {
        GeoDistance min = GeoDistance.parse("1m");
        GeoDistance max = GeoDistance.parse("2m");

        GeoTransformation transformation = new Buffer().maxDistance(max).minDistance(min);
        JtsGeometry geometry = geometry("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);

        JtsGeometry maxGeometry = geometry.getBuffered(max.getDegrees(), GeoShapeCondition.CONTEXT);
        JtsGeometry minGeometry = geometry.getBuffered(min.getDegrees(), GeoShapeCondition.CONTEXT);
        Geometry difference = maxGeometry.getGeom().difference(minGeometry.getGeom());
        JtsGeometry desiredGeometry = GeoShapeCondition.CONTEXT.makeShape(difference);

        assertEquals("Failed applied BufferTransformation WithPositiveDistances", desiredGeometry, transformedGeometry);
    }

    @Test
    public void testBufferTransformationWithNegativeDistances() {
        GeoDistance min = GeoDistance.parse("-1m");
        GeoDistance max = GeoDistance.parse("-2m");

        GeoTransformation transformation = new Buffer().maxDistance(max).minDistance(min);
        JtsGeometry geometry = geometry("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);

        JtsGeometry maxGeometry = geometry.getBuffered(max.getDegrees(), GeoShapeCondition.CONTEXT);
        JtsGeometry minGeometry = geometry.getBuffered(min.getDegrees(), GeoShapeCondition.CONTEXT);
        Geometry difference = maxGeometry.getGeom().difference(minGeometry.getGeom());
        JtsGeometry desiredGeometry = GeoShapeCondition.CONTEXT.makeShape(difference);

        assertEquals("Failed applied BufferTransformation WithNegativeDistances", desiredGeometry, transformedGeometry);
    }

    @Test
    public void testBufferTransformationWithInvertedPositiveDistances() {
        GeoDistance min = GeoDistance.parse("2m");
        GeoDistance max = GeoDistance.parse("1m");

        GeoTransformation transformation = new Buffer().maxDistance(max).minDistance(min);
        JtsGeometry geometry = geometry("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);

        JtsGeometry maxGeometry = geometry.getBuffered(max.getDegrees(), GeoShapeCondition.CONTEXT);
        JtsGeometry minGeometry = geometry.getBuffered(min.getDegrees(), GeoShapeCondition.CONTEXT);
        Geometry difference = maxGeometry.getGeom().difference(minGeometry.getGeom());
        JtsGeometry desiredGeometry = GeoShapeCondition.CONTEXT.makeShape(difference);

        assertEquals("Failed applied BufferTransformation WithInvertedPositiveDistances",
                     desiredGeometry,
                     transformedGeometry);
    }

    @Test
    public void testBufferTransformationWithInvertedNegativeDistances() {
        GeoDistance min = GeoDistance.parse("-1m");
        GeoDistance max = GeoDistance.parse("-2m");

        GeoTransformation transformation = new Buffer().maxDistance(max).minDistance(min);
        JtsGeometry geometry = geometry("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);

        JtsGeometry maxGeometry = geometry.getBuffered(max.getDegrees(), GeoShapeCondition.CONTEXT);
        JtsGeometry minGeometry = geometry.getBuffered(min.getDegrees(), GeoShapeCondition.CONTEXT);
        Geometry difference = maxGeometry.getGeom().difference(minGeometry.getGeom());
        JtsGeometry desiredGeometry = GeoShapeCondition.CONTEXT.makeShape(difference);

        assertEquals("Failed applied BufferTransformation WithInvertedNegativeDistances",
                     desiredGeometry,
                     transformedGeometry);
    }

    @Test
    public void testBufferTransformationToString() {
        GeoDistance min = GeoDistance.parse("-1m");
        GeoDistance max = GeoDistance.parse("-2m");
        GeoTransformation transformation = new Buffer().maxDistance(max).minDistance(min);
        assertEquals("Failed GeoTransformation.Buffer.toString ",
                     "Buffer{maxDistance=GeoDistance{value=-2.0, unit=METRES}, " +
                     "minDistance=GeoDistance{value=-1.0, unit=METRES}}",
                     transformation.toString());
    }

    @Test
    public void testBufferTransformationParsing() throws IOException {
        String json = "{type:\"buffer\",max_distance:\"1km\",min_distance:\"10m\"}";
        Buffer buffer = JsonSerializer.fromString(json, Buffer.class);
        assertNotNull("JSON serialization is wrong", buffer);
        assertEquals("JSON serialization is wrong", 1.0, buffer.maxDistance().getValue(GeoDistanceUnit.KILOMETRES));
        assertEquals("JSON serialization is wrong", 10.0, buffer.minDistance().getValue(GeoDistanceUnit.METRES));
    }

}
