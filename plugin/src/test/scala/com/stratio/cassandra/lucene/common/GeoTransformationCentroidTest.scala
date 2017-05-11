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
import com.stratio.cassandra.lucene.common.GeoTransformations.Centroid
import org.junit.Assert.{assertEquals, assertNotNull}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Unit tests for [[GeoTransformations.Centroid]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 */
@RunWith(classOf[JUnitRunner])
class GeoTransformationCentroidTest extends BaseScalaTest {

    test("test Centroid Transformation Point") {
        val shape = "POINT (-30.1 50.2)"
        val centroid = "POINT (-30.1 50.2)"

        val transformation = new Centroid()
        val geometry = GeospatialUtilsJTS.geometry(shape)
        val transformedGeometry = transformation.apply(geometry)
        assertEquals("Failed applied centroid transformation to point", centroid, transformedGeometry.toString)
    }

    test("test Centroid Transformation Line") {
        val shape = "LINESTRING (0 -30, 0 50)"
        val centroid = "POINT (0 10)"

        val transformation = new GeoTransformations.Centroid()
        val geometry = GeospatialUtilsJTS.geometry(shape)
        val transformedGeometry = transformation.apply(geometry)
        assertEquals("Failed applied centroid transformation to line", centroid, transformedGeometry.toString)
    }

    test("test Centroid Transformation Polygon") {
        val shape = "POLYGON ((-30 30, -30 0, 30 0, 30 30, -30 30))"
        val centroid = "POINT (-0 15)"

        val transformation = new GeoTransformations.Centroid()
        val geometry = GeospatialUtilsJTS.geometry(shape)
        val transformedGeometry = transformation.apply(geometry)
        assertEquals("Failed applied centroid transformation to polygon", centroid, transformedGeometry.toString)
    }

    test("test Centroid Transformation MultiPolygon") {
        val shape = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))"
        val centroid = "POINT (24.285714285714285 24.047619047619047)"

        val transformation = new GeoTransformations.Centroid()
        val geometry = GeospatialUtilsJTS.geometry(shape)
        val transformedGeometry = transformation.apply(geometry)
        assertEquals("Failed applied centroid transformation to multipart", centroid, transformedGeometry.toString)
    }


    test("test Centroid Transformation ToString") {
        val transformation = new GeoTransformations.Centroid()
        assertEquals("Centroid{}", transformation.toString())
    }

    test("test Centroid Transformation Parsing") {
        val json = "{type:\"centroid\"}"
        val centroid = JsonSerializer.fromString(json, classOf[Centroid])
        assertNotNull("JSON shape serialization is wrong", centroid)
    }

}
