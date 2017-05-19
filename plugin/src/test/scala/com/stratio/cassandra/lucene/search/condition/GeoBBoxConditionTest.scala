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
package com.stratio.cassandra.lucene.search.condition

import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.schema.SchemaBuilders._
import com.stratio.cassandra.lucene.search.SearchBuilders.geoBBox
import com.stratio.cassandra.lucene.search.condition.builder.GeoBBoxConditionBuilder
import org.apache.lucene.spatial.composite.IntersectsRPTVerifyQuery
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class GeoBBoxConditionTest extends AbstractConditionTest {

  test("Build") {
    val builder = new GeoBBoxConditionBuilder("name", -90D, 90D, -180D, 180D).boost(0.5f)
    val condition = builder.build
    assertEquals("Boost is not set", 0.5, condition.boost)
    assertEquals("Field is not set", "name", condition.field)
    assertEquals("Min longitude is not set", -180, condition.minLongitude, 0)
    assertEquals("Max longitude is not set", 180, condition.maxLongitude, 0)
    assertEquals("Min latitude is not set", -90, condition.minLatitude, 0)
    assertEquals("Max latitude is not set", 90, condition.maxLatitude, 0)
  }

  test("BuildDefaults") {
    val builder = new GeoBBoxConditionBuilder("name", 2D, 3D, 0D, 1D)
    val condition = builder.build
    assertNull("Boost is not set to default", condition.boost)
    assertEquals("Field is not set", "name", condition.field)
    assertEquals("Min longitude is not set", 0, condition.minLongitude, 0)
    assertEquals("Max longitude is not set", 1, condition.maxLongitude, 0)
    assertEquals("Min latitude is not set", 2, condition.minLatitude, 0)
    assertEquals("Max latitude is not set", 3, condition.maxLatitude, 0)
  }

  test("BuildNullField") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, null, 2D, 3D, 0D, 1D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildEmptyField") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, "", 2D, 3D, 0D, 1D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildBlankField") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, " ", 2D, 3D, 0D, 1D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildNullMinLongitude") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, "name", 2D, 3D, null, 1D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildTooSmallMinLongitude") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, "name", 2D, 3D, -181D, 1D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildTooLargeMinLongitude") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, "name", 2D, 3D, 181D, 1D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildNullMaxLongitude") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, "name", 2D, 3D, 0D, null)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildTooSmallMaxLongitude") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, "name", 2D, 3D, 0D, -181D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildTooLargeMaxLongitude") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, "name", 2D, 3D, 0D, 181D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildNullLatitude") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, "name", null, 3D, 0D, 1D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildTooSmallMinLatitude") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, "name", -91D, 3D, 0D, 1D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildTooLargeMinLatitude") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, "name", 91D, 3D, 0D, 1D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildNullMaxLatitude") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, "name", 2D, null, 0D, 1D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildTooSmallMaxLatitude") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, "name", 2D, -91D, 0D, 1D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildTooLargeMaxLatitude") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, "name", 2D, 91D, 0D, 1D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildMinLongitudeGreaterThanMaxLongitude") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, "name", 3D, 3D, 2D, 1D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildMinLatitudeGreaterThanMaxLatitude") {
    intercept[IndexException] {
      new GeoBBoxCondition(null, "name", 4D, 3D, 0D, 1D)
    }.getMessage shouldBe s"fdñljps"
  }

  test("QueryWithPointMapper") {
    val schemaVal = schema().mapper("name", geoPointMapper("lat", "lon").maxLevels(8)).build
    val condition = new GeoBBoxCondition(0.5f, "name", -90D, 90D, -180D, 180D)
    val query = condition.doQuery(schemaVal)
    assertNotNull("Query is wrong is not built", query)
    assertEquals("Query type is wrong", classOf[IntersectsRPTVerifyQuery], query.getClass)
  }

  test("QueryWithShapeMapper") {
    val schemaVal = schema().mapper("name", geoShapeMapper().maxLevels(8)).build
    val condition = new GeoBBoxCondition(0.5f, "name", -90D, 90D, -180D, 180D)
    val query = condition.doQuery(schemaVal)
    assertNotNull("Query is wrong is not built", query)
    assertEquals("Query type is wrong", classOf[IntersectsRPTVerifyQuery], query.getClass)
  }

  test("QueryWithInvalidMapper") {
    val schemaVal = schema().mapper("name", uuidMapper()).build
    val condition = new GeoBBoxCondition(0.5f, "name", -90D, 90D, -180D, 180D)
    condition.query(schemaVal)
  }

  test("ToString") {
    val condition = geoBBox("name", -180D, 180D, -90D, 90D).boost(0.5f).build
    assertEquals("Method #toString is wrong",
      "GeoBBoxCondition{boost=0.5, field=name, " +
        "minLatitude=-90.0, maxLatitude=90.0, minLongitude=-180.0, maxLongitude=180.0}",
      condition.toString())
  }
}
