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
import org.apache.lucene.spatial.query.SpatialOperation
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * @author Eduardo Alonso `eduardoalonso@stratio.com`
 */
@RunWith(classOf[JUnitRunner])
class GeoOperationTest extends BaseScalaTest {

    test("parse null") {
        try {
            GeoOperation.parse(null)
            fail("expected IndexException but not.")
        } catch {
            case iE: IndexException =>
            case _ => fail("expected IndexException but not.")
        }
    }

    test("parse null 2") {
        assertThrows[IndexException](GeoOperation.parse(null))
    }

    test("parse empty") {
        try {
            GeoOperation.parse("")
            fail("expected IndexException but not.")
        } catch {
            case iE: IndexException =>
            case _ => fail("expected IndexException but not.")
        }
    }

    test("parse blank") {
        try {
            GeoOperation.parse("\t ")
            fail("expected IndexException but not.")
        } catch {
            case iE: IndexException =>
            case _ => fail("expected IndexException but not.")
        }
    }

    test("parse invalid") {
        try {
            GeoOperation.parse("invalid_operation")
            fail("expected IndexException but not.")
        } catch {
            case iE: IndexException =>
            case _ => fail("expected IndexException but not.")
        }
    }

    test("parse intersects") {
        val operation : GeoOperation = GeoOperation.parse("intersects")
        assertEquals("invalid GeoOperation parsing", operation.spatialOperation, SpatialOperation.Intersects)
    }

    test("parse is_within") {
        val operation : GeoOperation = GeoOperation.parse("is_within")
        assertEquals("invalid GeoOperation parsing", operation.spatialOperation, SpatialOperation.IsWithin)
    }

    test("parse contains") {
        val operation : GeoOperation = GeoOperation.parse("contains")
        assertEquals("invalid GeoOperation parsing", operation.spatialOperation, SpatialOperation.Contains)
    }
}
