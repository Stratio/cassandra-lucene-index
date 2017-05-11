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
package com.stratio.cassandra.lucene.common

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.common.GeoTransformations.Buffer
import org.junit.Assert.{assertEquals, assertNotNull}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Unit tests for [[GeoTransformations.Buffer]].
 *
 * @author Eduardo Alonso `eduardoalonso@stratio.com`
 */
@RunWith(classOf[JUnitRunner])
class GeoTransformationBufferTest extends BaseScalaTest {

    test("test Buffer Transformation With Null MaxDistance") {
        val min = GeoDistance.parse("1m")

        val transformation = new Buffer(min, null)
        val geometry = GeospatialUtilsJTS.geometry("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))")
        val transformedGeometry = transformation.apply(geometry)

        val max = GeospatialUtilsJTS.CONTEXT.makeShape(geometry.getGeom)
        val minGeometry = geometry.getBuffered(min.getDegrees, GeospatialUtilsJTS.CONTEXT)
        val difference = max.getGeom.difference(minGeometry.getGeom)
        val desiredGeometry = GeospatialUtilsJTS.CONTEXT.makeShape(difference)
        assertEquals("Failed applied BufferTransformation", desiredGeometry, transformedGeometry)
    }

    test("test Buffer Transformation With Null MinDistance") {
        val max = GeoDistance.parse("2m")

        val transformation = new Buffer(null, max)
        val geometry = GeospatialUtilsJTS.geometry("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))")
        val transformedGeometry = transformation.apply(geometry)

        val desiredGeometry = geometry.getBuffered(max.getDegrees, GeospatialUtilsJTS.CONTEXT)
        assertEquals("Failed applied BufferTransformation", desiredGeometry, transformedGeometry)
    }

    test("test Buffer Transformation With Null MinMaxDistance") {
        val transformation = new Buffer(null, null)
        val geometry = GeospatialUtilsJTS.geometry("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))")
        val transformedGeometry = transformation.apply(geometry)
        assertEquals("Applied BufferTransformation with min and max to null must return a equals geometry",
                     geometry,
                     transformedGeometry)
    }

    test("test Buffer Transformation With Positive Distances") {
        val min = GeoDistance.parse("1m")
        val max = GeoDistance.parse("2m")

        val transformation = new Buffer(min, max)
        val geometry = GeospatialUtilsJTS.geometry("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))")
        val transformedGeometry = transformation.apply(geometry)

        val maxGeometry = geometry.getBuffered(max.getDegrees, GeospatialUtilsJTS.CONTEXT)
        val minGeometry = geometry.getBuffered(min.getDegrees, GeospatialUtilsJTS.CONTEXT)
        val difference = maxGeometry.getGeom.difference(minGeometry.getGeom)
        val desiredGeometry = GeospatialUtilsJTS.CONTEXT.makeShape(difference)
        assertEquals("Failed applied BufferTransformation WithPositiveDistances", desiredGeometry, transformedGeometry)
    }

    test("test Buffer Transformation With Negative Distances") {
        val min = GeoDistance.parse("-1m")
        val max = GeoDistance.parse("-2m")

        val transformation = new Buffer(min, max)
        val geometry = GeospatialUtilsJTS.geometry("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))")
        val transformedGeometry = transformation.apply(geometry)

        val maxGeometry = geometry.getBuffered(max.getDegrees, GeospatialUtilsJTS.CONTEXT)
        val minGeometry = geometry.getBuffered(min.getDegrees, GeospatialUtilsJTS.CONTEXT)
        val difference = maxGeometry.getGeom.difference(minGeometry.getGeom)
        val desiredGeometry = GeospatialUtilsJTS.CONTEXT.makeShape(difference)
        assertEquals("Failed applied BufferTransformation WithNegativeDistances", desiredGeometry, transformedGeometry)
    }

    test("test Buffer Transformation With Inverted Positive Distances") {
        val min = GeoDistance.parse("2m")
        val max = GeoDistance.parse("1m")

        val transformation = new Buffer(min, max)
        val geometry = GeospatialUtilsJTS.geometry("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))")
        val transformedGeometry = transformation.apply(geometry)

        val maxGeometry = geometry.getBuffered(max.getDegrees, GeospatialUtilsJTS.CONTEXT)
        val minGeometry = geometry.getBuffered(min.getDegrees, GeospatialUtilsJTS.CONTEXT)
        val difference = maxGeometry.getGeom.difference(minGeometry.getGeom)
        val desiredGeometry = GeospatialUtilsJTS.CONTEXT.makeShape(difference)
        assertEquals("Failed applied BufferTransformation WithInvertedPositiveDistances",
                     desiredGeometry,
                     transformedGeometry)
    }

    test("testBufferTransformationWithInvertedNegativeDistances") {
        val min = GeoDistance.parse("-1m")
        val max = GeoDistance.parse("-2m")

        val transformation = new Buffer(min, max)
        val geometry = GeospatialUtilsJTS.geometry("POLYGON((1 1,5 1,5 5,1 5,1 1),(2 2, 3 2, 3 3, 2 3,2 2))")
        val transformedGeometry = transformation.apply(geometry)

        val maxGeometry = geometry.getBuffered(max.getDegrees, GeospatialUtilsJTS.CONTEXT)
        val minGeometry = geometry.getBuffered(min.getDegrees, GeospatialUtilsJTS.CONTEXT)
        val difference = maxGeometry.getGeom.difference(minGeometry.getGeom)
        val desiredGeometry = GeospatialUtilsJTS.CONTEXT.makeShape(difference)
        assertEquals("Failed applied BufferTransformation WithInvertedNegativeDistances",
                     desiredGeometry,
                     transformedGeometry)
    }

    test("test Buffer Transformation ToString") {
        val min = GeoDistance.parse("-1m")
        val max = GeoDistance.parse("-2m")
        val transformation = new Buffer(min, max)
        assertEquals("Failed GeoTransformation.Buffer.toString ",
                     "Buffer{" +
                     "minDistance=GeoDistance{value=-1.0, unit=METRES}, " +
                     "maxDistance=GeoDistance{value=-2.0, unit=METRES}}",
                     transformation.toString())
    }

    test("test Buffer Transformation Parsing") {
        val json = "{type:\"buffer\",max_distance:\"1km\",min_distance:\"10m\"}"
        val buffer = JsonSerializer.fromString(json, classOf[Buffer])
        assertNotNull("JSON serialization is wrong", buffer)
        assertEquals("JSON min distance serialization is wrong", 10.0, buffer.minDistance.getValue(GeoDistanceUnit.CENTIMETRES), 0)
        assertEquals("JSON max distance serialization is wrong", 1.0, buffer.maxDistance.getValue(GeoDistanceUnit.KILOMETRES), 0)
    }

}
