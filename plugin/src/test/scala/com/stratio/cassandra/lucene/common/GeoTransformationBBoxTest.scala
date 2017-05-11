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
import com.stratio.cassandra.lucene.common.GeoTransformations.BBox
import org.junit.Assert.{assertEquals, assertNotNull}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Unit tests for [[BBox]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 */
@RunWith(classOf[JUnitRunner])
class GeoTransformationBBoxTest extends BaseScalaTest {

    test("bounding box transformation point") {
        val shape = "POINT (-30.1 50.2)"
        val bbox = "POINT (-30.1 50.2)"

        val transformation = new BBox()
        val geometry = GeospatialUtilsJTS.geometry(shape)
        val transformedGeometry = transformation.apply(geometry)
        assertEquals("Failed applied bbox transformation to point", bbox, transformedGeometry.toString)
    }


    test("bounding box transformation line") {
        val shape = "LINESTRING (0 -30, 0 50)"
        val bbox = "LINESTRING (0 -30, 0 50)"

        val transformation = new BBox()
        val geometry = GeospatialUtilsJTS.geometry(shape)
        val transformedGeometry = transformation.apply(geometry)
        assertEquals("Failed applied bbox transformation to line", bbox, transformedGeometry.toString)
    }

    test("bounding box transformation polygon") {
        val shape = "POLYGON ((-30 30, -30 0, 30 0, 30 30, -30 30))"
        val bbox = "POLYGON ((-30 0, -30 30, 30 30, 30 0, -30 0))"

        val transformation = new BBox()
        val geometry = GeospatialUtilsJTS.geometry(shape)
        val transformedGeometry = transformation.apply(geometry)
        assertEquals("Failed applied bbox transformation to polygon", bbox, transformedGeometry.toString)
    }

    test("bounding box transformation multipolygon") {
        val shape = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)),((15 5, 40 10, 10 20, 5 10, 15 5)))"
        val bbox = "POLYGON ((5 5, 5 40, 45 40, 45 5, 5 5))"

        val transformation = new BBox()
        val geometry = GeospatialUtilsJTS.geometry(shape)
        val transformedGeometry = transformation.apply(geometry)
        assertEquals("Failed applied bbox transformation to multipart", bbox, transformedGeometry.toString)
    }

    test("bounding box transformation toString") {
        val transformation = new BBox()
        assertEquals("BBox{}", transformation.toString())
    }

    test("bounding box transformation parsing") {
        val  json = "{type:\"bbox\"}"
        val bbox = JsonSerializer.fromString(json, classOf[BBox])
        assertNotNull("JSON shape serialization is wrong", bbox)
    }
}
