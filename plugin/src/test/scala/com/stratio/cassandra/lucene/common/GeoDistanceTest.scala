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

import com.stratio.cassandra.lucene.{BaseScalaTest, IndexException}
import org.junit.Assert.assertEquals

/**
 * @author Andres de la Pena `adelapena@stratio.com`
 */
class GeoDistanceTest extends BaseScalaTest {

    test("parse null") {
        try {
            GeoDistance.parse(null)
            fail("expected IndexException but not.")
        } catch {
            case iE: IndexException =>
            case _ => fail("expected IndexException but not.")
        }
    }
    test("parse empty") {
        try {
            GeoDistance.parse("")
            fail("expected IndexException but not.")
        } catch {
            case iE: IndexException =>
            case _ => fail("expected IndexException but not.")
        }
    }

    test("parse blank") {
        try {
            GeoDistance.parse("\t ")
            fail("expected IndexException but not.")
        } catch {
            case iE: IndexException =>
            case _ => fail("expected IndexException but not.")
        }
    }


    test("parse millimeters") {
        GeoDistanceTest.check("4mm",0.004)
        GeoDistanceTest.check("0.4millimetres",0.0004)
    }

    test("parse centimetres") {
        GeoDistanceTest.check("4cm", 0.04)
        GeoDistanceTest.check("0.4centimetres", 0.004)
    }

    test("parse decimetres") {
        GeoDistanceTest.check("4dm", 0.4)
        GeoDistanceTest.check("0.4decimetres", 0.04)
    }

    test("parse metres") {
        GeoDistanceTest.check("2m", 2)
        GeoDistanceTest.check("0.2metres", 0.2)
    }

    test("parse decametres") {
        GeoDistanceTest.check("2dam", 20)
        GeoDistanceTest.check("0.2decametres", 2)
    }

    test("parse hectometres") {
        GeoDistanceTest.check("2hm", 200)
        GeoDistanceTest.check("0.2hectometres", 20)
    }

    test("parse kilometres") {
        GeoDistanceTest.check("2km", 2000)
        GeoDistanceTest.check("0.2kilometres", 200)
    }

    test("parse foots") {
        GeoDistanceTest.check("2ft", 0.6096)
        GeoDistanceTest.check("0.2foots", 0.06096)
    }

    test("parse yards") {
        GeoDistanceTest.check("2yd", 1.8288)
        GeoDistanceTest.check("0.2yards", 0.18288)
    }

    test("parse inches") {
        GeoDistanceTest.check("2in", 0.0508)
        GeoDistanceTest.check("0.2inches", 0.00508)
    }

    test("parse miles") {
        GeoDistanceTest.check("2mi", 3218.688)
        GeoDistanceTest.check("0.2miles", 321.8688)
    }

    test("parse nautical miles") {
        GeoDistanceTest.check("2M", 3700.0)
        GeoDistanceTest.check("0.2NM", 370.0)
        GeoDistanceTest.check("0.2mil", 370.0)
        GeoDistanceTest.check("0.2nautical_miles", 370.0)
    }
}

object GeoDistanceTest {
    def check(json_value:String, expected: Double): Unit = {
        val distance : GeoDistance = GeoDistance.parse(json_value)
        assertEquals("invalid GeoDistance parsing", expected, distance.getValue(GeoDistanceUnit.METRES), 0.0000000001)
    }
}