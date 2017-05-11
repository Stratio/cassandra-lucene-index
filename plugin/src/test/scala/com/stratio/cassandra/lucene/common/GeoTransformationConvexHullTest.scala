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

import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.common.GeoTransformations.ConvexHull
import org.junit.Assert.{assertEquals, assertNotNull}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Unit tests for [[ConvexHull]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 */
@RunWith(classOf[JUnitRunner])
class GeoTransformationConvexHullTest extends BaseScalaTest {

    test("test ConvexHull Transformation Point") {
        val shape = "POINT (-30.1 50.2)"
        val convexHull = "POINT (-30.1 50.2)"

        val transformation = new ConvexHull()
        val geometry = GeospatialUtilsJTS.geometry(shape)
        val transformedGeometry = transformation.apply(geometry)
        assertEquals("Failed applied convex hull transformation to point", convexHull, transformedGeometry.toString)
    }

    test("test ConvexHull Transformation Line") {
        val shape = "LINESTRING (0 -30, 0 50)"
        val convexHull = "LINESTRING (0 -30, 0 50)"

        val transformation = new ConvexHull()
        val geometry = GeospatialUtilsJTS.geometry(shape)
        val transformedGeometry = transformation.apply(geometry)
        assertEquals("Failed applied convex hull transformation to line", convexHull, transformedGeometry.toString)
    }

    test("test ConvexHull Transformation Polygon") {
        val shape = "POLYGON ((-30 30, -30 0, 30 0, 30 30, -30 30))"
        val convexHull = "POLYGON ((-30 0, -30 30, 30 30, 30 0, -30 0))"

        val transformation = new ConvexHull()
        val geometry = GeospatialUtilsJTS.geometry(shape)
        val transformedGeometry = transformation.apply(geometry)
        assertEquals("Failed applied convex hull transformation to polygon",
                     convexHull,
                     transformedGeometry.toString)
    }

    test("test ConvexHull Transformation MultiPolygon") {
        val shape = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))"
        val convexHull = "POLYGON ((15 5, 5 10, 10 40, 45 40, 40 10, 15 5))"

        val transformation = new ConvexHull()
        val geometry = GeospatialUtilsJTS.geometry(shape)
        val transformedGeometry = transformation.apply(geometry)
        assertEquals("Failed applied convex hull transformation to multipart",
                     convexHull,
                     transformedGeometry.toString)
    }

    test("test ConvexHull Transformation ToString") {
        val transformation = new ConvexHull()
        assertEquals("ConvexHull{}", transformation.toString)
    }

    test("test ConvexHull Transformation Parsing") {
        val json = "{type:\"convex_hull\"}"
        val convexHull = JsonSerializer.fromString(json, classOf[ConvexHull])
        assertNotNull("JSON shape serialization is wrong", convexHull)
    }
}
