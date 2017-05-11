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

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.common.GeoShapes._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.junit.Assert.assertEquals

/**
 * Class for testing [[GeoShapes]]s.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 */
@RunWith(classOf[JUnitRunner])
class GeoShapeTest extends BaseScalaTest {

    test("WKT") {
        val shape : GeoShapes.GeoShape = BaseScalaTest.parse[GeoShapes.GeoShape]("{type:\"wkt\", value:\"POINT(0 0)\"}", classOf[GeoShapes.GeoShape])
        assertEquals("Geo shape parsing is wrong", classOf[WKT], shape.getClass)
        val wkt : WKT = shape.asInstanceOf[WKT]
        assertEquals("Geo shape parsing is wrong", "POINT(0 0)", wkt.value)
        assertEquals("Geo shape parsing is wrong", "WKT{value=POINT(0 0)}", wkt.toString)
        assertEquals("Geo shape calculation is wrong", "POINT (0 0)", wkt.apply.toString)
    }

    test("BBox") {
        val shape : GeoShapes.GeoShape = BaseScalaTest.parse[GeoShapes.GeoShape]("{type:\"bbox\", shape:{type:\"wkt\", value:\"POLYGON((-1 1, 1 1, -1 -1, -1 1))\"}}", classOf[GeoShapes.GeoShape])
        assertEquals("Geo shape parsing is wrong", classOf[BBox], shape.getClass)
        val  bbox : BBox = shape.asInstanceOf[BBox]
        assertEquals("BBox geo shape calculation is wrong", "POLYGON ((-1 -1, -1 1, 1 1, 1 -1, -1 -1))", bbox.apply.toString)
    }

    test("Centroid") {
        val shape : GeoShapes.GeoShape = BaseScalaTest.parse[GeoShapes.GeoShape]("{type:\"centroid\", " +
                               "shape:{type:\"wkt\", value:\"POLYGON((-1 1, 1 1, 1 -1, -1 -1, -1 1))\"}}", classOf[GeoShapes.GeoShape])
        assertEquals("Geo shape parsing is wrong", classOf[Centroid], shape.getClass)
        val centroid : Centroid = shape.asInstanceOf[Centroid]
        assertEquals("Centroid geo shape calculation is wrong", "POINT (-0 -0)", centroid.apply.toString)
    }

    test("Buffer") {
        val shape : GeoShapes.GeoShape = BaseScalaTest.parse[GeoShapes.GeoShape]("{type:\"buffer\", max_distance:\"1000km\", " +
                               "shape:{type:\"wkt\", value:\"POINT(0 0)\"}}", classOf[GeoShapes.GeoShape])
        assertEquals("Buffer geo shape parsing is wrong", classOf[Buffer], shape.getClass)
        val buffer : Buffer = shape.asInstanceOf[Buffer]
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
                     "8.993203677616636 0))", buffer.apply.toString)
    }

    test("Convexhull") {
        val shape : GeoShapes.GeoShape = BaseScalaTest.parse[GeoShapes.GeoShape]("{type:\"convex_hull\", shape:{type:\"wkt\", value:\"POINT(0 0)\"}}", classOf[GeoShapes.GeoShape])
        assertEquals("Buffer geo shape parsing is wrong", classOf[ConvexHull], shape.getClass)
        val convexHull = shape.asInstanceOf[ConvexHull]
    }

    test("difference") {
        val shape : GeoShapes.GeoShape = BaseScalaTest.parse[GeoShapes.GeoShape]("{type:\"difference\", shapes: [{type:\"wkt\", value:\"POINT(0 0)\"},{type:\"wkt\", value:\"POINT(0 0)\"}]}", classOf[GeoShapes.GeoShape])
        assertEquals("Buffer geo shape parsing is wrong", classOf[Difference], shape.getClass)
    }

    test("intersection") {
        val shape : GeoShapes.GeoShape = BaseScalaTest.parse[GeoShapes.GeoShape]("{type:\"intersection\", shapes: [{type:\"wkt\", value:\"POINT(0 0)\"},{type:\"wkt\", value:\"POINT(0 0)\"}]}", classOf[GeoShapes.GeoShape])
        assertEquals("Buffer geo shape parsing is wrong", classOf[Intersection], shape.getClass)
        val intersection = shape.asInstanceOf[Intersection]
        assertEquals("intersection(A, A)==A ", "POINT (0 0)",intersection.apply.toString)
    }

    test("union") {
        val shape : GeoShapes.GeoShape = BaseScalaTest.parse[GeoShapes.GeoShape]("{type:\"union\", shapes: [{type:\"wkt\", value:\"POINT(0 0)\"},{type:\"wkt\", value:\"POINT(0 0)\"}]}", classOf[GeoShapes.GeoShape])
        assertEquals("Buffer geo shape parsing is wrong", classOf[Union], shape.getClass)
        val union = shape.asInstanceOf[Union]
        assertEquals("union(A, A)==A ", "POINT (0 0)",union.apply.toString)
    }





}