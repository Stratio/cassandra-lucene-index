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

import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Geometry;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoTransformationTest extends AbstractConditionTest {

    @Test
    public void testCopyTransformation() {
        GeoTransformation transformation = new GeoTransformation.Copy();
        JtsGeometry geometry = GeoShapeCondition.geometryFromWKT(
                "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);
        assertEquals("Applied CopyTransformation to a geometry must return a equals JTSGeometry",
                     geometry,
                     transformedGeometry);
    }

    @Test
    public void testBufferTransformationWithNullMaxDistance() {
        GeoDistance minDistance = GeoDistance.parse("1m");

        GeoTransformation transformation = new GeoTransformation.Buffer(null, minDistance);
        JtsGeometry geometry = GeoShapeCondition.geometryFromWKT(
                "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);

        JtsGeometry max = GeoShapeCondition.CONTEXT.makeShape(geometry.getGeom());
        JtsGeometry minGeometry = geometry.getBuffered(minDistance.getDegrees(), GeoShapeCondition.CONTEXT);
        Geometry difference = max.getGeom().difference(minGeometry.getGeom());
        JtsGeometry desiredGeometry = GeoShapeCondition.CONTEXT.makeShape(difference);

        assertEquals("Failed applied BufferTransformation", desiredGeometry, transformedGeometry);
    }

    @Test
    public void testBufferTransformationWithNullMinDistance() {
        GeoDistance maxDistance = GeoDistance.parse("2m");

        GeoTransformation transformation = new GeoTransformation.Buffer(maxDistance, null);
        JtsGeometry geometry = GeoShapeCondition.geometryFromWKT(
                "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);

        JtsGeometry desiredGeometry = geometry.getBuffered(maxDistance.getDegrees(), GeoShapeCondition.CONTEXT);
        assertEquals("Failed applied BufferTransformation", desiredGeometry, transformedGeometry);
    }

    @Test
    public void testBufferTransformationWithNullMinMaxDistance() {
        GeoTransformation transformation = new GeoTransformation.Buffer(null, null);
        JtsGeometry geometry = GeoShapeCondition.geometryFromWKT(
                "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);
        assertEquals("Applied BufferTransformation with min and max to null must return a equals geometry",
                     geometry,
                     transformedGeometry);
    }

    @Test
    public void testBufferTransformationWithPositiveDistances() {
        GeoDistance minDistance = GeoDistance.parse("1m");
        GeoDistance maxDistance = GeoDistance.parse("2m");

        GeoTransformation transformation = new GeoTransformation.Buffer(maxDistance, minDistance);
        JtsGeometry geometry = GeoShapeCondition.geometryFromWKT(
                "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);

        JtsGeometry max = geometry.getBuffered(maxDistance.getDegrees(), GeoShapeCondition.CONTEXT);
        JtsGeometry min = geometry.getBuffered(minDistance.getDegrees(), GeoShapeCondition.CONTEXT);
        Geometry difference = max.getGeom().difference(min.getGeom());
        JtsGeometry desiredGeometry = GeoShapeCondition.CONTEXT.makeShape(difference);

        assertEquals("Failed applied BufferTransformation WithPositiveDistances", desiredGeometry, transformedGeometry);

    }

    @Test
    public void testBufferTransformationWithNegativeDistances() {
        GeoDistance minDistance = GeoDistance.parse("-1m");
        GeoDistance maxDistance = GeoDistance.parse("-2m");

        GeoTransformation transformation = new GeoTransformation.Buffer(maxDistance, minDistance);
        JtsGeometry geometry = GeoShapeCondition.geometryFromWKT(
                "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);

        JtsGeometry max = geometry.getBuffered(maxDistance.getDegrees(), GeoShapeCondition.CONTEXT);
        JtsGeometry min = geometry.getBuffered(minDistance.getDegrees(), GeoShapeCondition.CONTEXT);
        Geometry difference = max.getGeom().difference(min.getGeom());
        JtsGeometry desiredGeometry = GeoShapeCondition.CONTEXT.makeShape(difference);

        assertEquals("Failed applied BufferTransformation WithNegativeDistances", desiredGeometry, transformedGeometry);
    }

    @Test
    public void testBufferTranformationWithInvertedPositiveDistances() {
        GeoDistance minDistance = GeoDistance.parse("2m");
        GeoDistance maxDistance = GeoDistance.parse("1m");

        GeoTransformation transformation = new GeoTransformation.Buffer(maxDistance, minDistance);
        JtsGeometry geometry = GeoShapeCondition.geometryFromWKT(
                "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);

        JtsGeometry max = geometry.getBuffered(maxDistance.getDegrees(), GeoShapeCondition.CONTEXT);
        JtsGeometry min = geometry.getBuffered(minDistance.getDegrees(), GeoShapeCondition.CONTEXT);
        Geometry difference = max.getGeom().difference(min.getGeom());
        JtsGeometry desiredGeometry = GeoShapeCondition.CONTEXT.makeShape(difference);

        assertEquals("Failed applied BufferTransformation WithInvertedPositiveDistances",
                     desiredGeometry,
                     transformedGeometry);
    }

    @Test
    public void testBufferTransformationWithInvertedNegativeDistances() {
        GeoDistance minDistance = GeoDistance.parse("-1m");
        GeoDistance maxDistance = GeoDistance.parse("-2m");

        GeoTransformation transformation = new GeoTransformation.Buffer(maxDistance, minDistance);
        JtsGeometry geometry = GeoShapeCondition.geometryFromWKT(
                "POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))");
        JtsGeometry transformedGeometry = transformation.apply(geometry, GeoShapeCondition.CONTEXT);

        JtsGeometry max = geometry.getBuffered(maxDistance.getDegrees(), GeoShapeCondition.CONTEXT);
        JtsGeometry min = geometry.getBuffered(minDistance.getDegrees(), GeoShapeCondition.CONTEXT);
        Geometry difference = max.getGeom().difference(min.getGeom());
        JtsGeometry desiredGeometry = GeoShapeCondition.CONTEXT.makeShape(difference);

        assertEquals("Failed applied BufferTransformation WithInvertedNegativeDistances",
                     desiredGeometry,
                     transformedGeometry);
    }

    @Test
    public void testToString() {
        GeoDistance minDistance = GeoDistance.parse("-1m");
        GeoDistance maxDistance = GeoDistance.parse("-2m");
        GeoTransformation transformation = new GeoTransformation.Buffer(maxDistance, minDistance);
        assertEquals("Failed GeoTransformation.toString ","",transformation.toString());
    }



}
