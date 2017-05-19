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
import com.stratio.cassandra.lucene.common.GeoDistance
import com.stratio.cassandra.lucene.schema.Schema
import com.stratio.cassandra.lucene.schema.SchemaBuilders._
import com.stratio.cassandra.lucene.search.SearchBuilders.geoDistance
import org.apache.lucene.search.{BooleanClause, BooleanQuery, Query}
import org.apache.lucene.spatial.composite.IntersectsRPTVerifyQuery
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class GeoDistanceConditionTest extends AbstractConditionTest {

  test("Constructor") {
    val condition = new GeoDistanceCondition(0.5f,
      "name",
      90D,
      -180D,
      GeoDistance.parse("3km"),
      GeoDistance.parse("10km"))
    assertEquals("Boost is not set", 0.5, condition.boost)
    assertEquals("Field is not set", "name", condition.field)
    assertEquals("Longitude is not set", -180, condition.longitude, 0)
    assertEquals("Latitude is not set", 90, condition.latitude, 0)
    assertEquals("Min distance is not set", GeoDistance.parse("3km"), condition.minDistance)
    assertEquals("Max distance is not set", GeoDistance.parse("10km"), condition.maxDistance)
  }

  test("ConstructorWithDefaults") {
    val condition = new GeoDistanceCondition(null,
      "name",
      90D,
      -180D,
      null,
      GeoDistance.parse("1yd"))
    assertNull("Boost is not set to default", condition.boost)
    assertEquals("Field is not set", "name", condition.field)
    assertEquals("Longitude is not set", -180, condition.longitude, 0)
    assertEquals("Latitude is not set", 90, condition.latitude, 0)
    assertNull("Min distance is not set", condition.minDistance)
    assertEquals("Max distance is not set", GeoDistance.parse("1yd"), condition.maxDistance)
  }

  test("ConstructorWithNullField") {
    intercept[IndexException] {
      new GeoDistanceCondition(null,
        null,
        90D,
        -180D,
        GeoDistance.parse("1km"),
        GeoDistance.parse("3km"))
    }.getMessage shouldBe s"fdñljps"
  }

  test("ConstructorWithEmptyField") {
    intercept[IndexException] {
      new GeoDistanceCondition(null,
        "",
        90D,
        -180D,
        GeoDistance.parse("1km"),
        GeoDistance.parse("3km"))
    }.getMessage shouldBe s"fdñljps"
  }

  test("ConstructorWithBlankField") {
    intercept[IndexException] {
      new GeoDistanceCondition(null,
        " ",
        90D,
        -180D,
        GeoDistance.parse("1km"),
        GeoDistance.parse("3km"))
    }.getMessage shouldBe s"fdñljps"
  }

  test("ConstructorWithNullLongitude") {
    intercept[IndexException] {
      new GeoDistanceCondition(null,
        "name",
        90D,
        null,
        GeoDistance.parse("1km"),
        GeoDistance.parse("3km"))
    }.getMessage shouldBe s"fdñljps"
  }

  test("ConstructorWithToSmallLongitude") {
    intercept[IndexException] {
      new GeoDistanceCondition(null,
        "name",
        90D,
        -181D,
        GeoDistance.parse("1km"),
        GeoDistance.parse("3km"))
    }.getMessage shouldBe s"fdñljps"
  }

  test("ConstructorWithToBigLongitude") {
    intercept[IndexException] {
      new GeoDistanceCondition(null,
        "name",
        90D,
        181D,
        GeoDistance.parse("1km"),
        GeoDistance.parse("3km"))
    }.getMessage shouldBe s"fdñljps"
  }

  test("ConstructorWithNullLatitude") {
    intercept[IndexException] {
      new GeoDistanceCondition(null,
        "name",
        null,
        -180D,
        GeoDistance.parse("1km"),
        GeoDistance.parse("3km"))
    }.getMessage shouldBe s"fdñljps"
  }

  test("ConstructorWithTooSmallLatitude") {
    intercept[IndexException] {
      new GeoDistanceCondition(null,
        "name",
        -91D,
        -180D,
        GeoDistance.parse("1km"),
        GeoDistance.parse("3km"))
    }.getMessage shouldBe s"fdñljps"
  }

  test("ConstructorWithTooBigLatitude") {
    intercept[IndexException] {
      new GeoDistanceCondition(null,
        "name",
        91D,
        -180D,
        GeoDistance.parse("1km"),
        GeoDistance.parse("3km"))
    }.getMessage shouldBe s"fdñljps"
  }

  test("ConstructorWithoutDistances") {
    intercept[IndexException] {
      new GeoDistanceCondition(null, "name", 90D, -180D, null, null)
    }.getMessage shouldBe s"fdñljps"
  }

  test("ConstructorWithMinLongitudeGreaterThanMaxLongitude") {
    intercept[IndexException] {
      new GeoDistanceCondition(null,
        "name",
        90D,
        -180D,
        GeoDistance.parse("10km"),
        GeoDistance.parse("3km"))
    }.getMessage shouldBe s"fdñljps"
  }

  test("QueryMax") {
    val schemaVal = schema().mapper("name", geoPointMapper("lat", "lon").maxLevels(8)).build
    val condition = new GeoDistanceCondition(0.5f,
      "name",
      90D,
      -180D,
      null,
      GeoDistance.parse("10hm"))
    var query: Query = condition.doQuery(schemaVal)
    assertNotNull("Query is not built", query)
    val booleanQuery = query.asInstanceOf[BooleanQuery]
    assertEquals("Query num clauses is wrong", 1, booleanQuery.clauses().size())
    val maxClause = booleanQuery.clauses().get(0)
    assertEquals("Query occur is wrong", BooleanClause.Occur.FILTER, maxClause.getOccur)
    query = maxClause.getQuery()
    assertEquals("Query type is wrong", classOf[IntersectsRPTVerifyQuery], query.getClass)
    assertEquals("Query is wrong", "IntersectsVerified(fieldName=)", query.toString())
  }

  test("QueryMin") {
    intercept[IndexException] {
      new GeoDistanceCondition(0.5f, "name", 90D, -180D, GeoDistance.parse("3km"), null)
    }.getMessage shouldBe s"fdñljps"
  }

  test("QueryMinMaxWithPointMapper") {
    GeoDistanceConditionTest.testQueryMinMaxWithValidSchema(schema().mapper("name",
      geoPointMapper("lat", "lon").maxLevels(8)).build)
  }

  test("QueryMinMaxWithShapeMapper") {
    GeoDistanceConditionTest.testQueryMinMaxWithValidSchema(schema().mapper("name",
      geoShapeMapper().maxLevels(8)).build)
  }



  test("QueryWithoutValidMapper") {
    intercept[IndexException] {
      val schemaVal = schema().mapper("name", uuidMapper()).build()
      val condition = new GeoDistanceCondition(0.5f,
        "name",
        90D,
        -180D,
        null,
        GeoDistance.parse("3km"))
      condition.query(schemaVal)
    }.getMessage shouldBe s"fdñljps"
  }

  test("ToString") {
    val condition = geoDistance("name", -1D, 9, "3km").minDistance(GeoDistance.parse("1km")).boost(
      0.4f).build
    assertEquals("Method #toString is wrong",
      "GeoDistanceCondition{boost=0.4, field=name, " +
        "latitude=9.0, longitude=-1.0, " +
        "minGeoDistance=GeoDistance{value=1.0, unit=KILOMETRES}, " +
        "maxGeoDistance=GeoDistance{value=3.0, unit=KILOMETRES}}",
      condition.toString())
  }
}

object GeoDistanceConditionTest {
  def testQueryMinMaxWithValidSchema(schema: Schema): Unit = {
    val condition = new GeoDistanceCondition(0.5f, "name",
      90D,
      -180D,
      GeoDistance.parse("1km"),
      GeoDistance.parse("3km"))
    var query = condition.doQuery(schema)
    assertNotNull("Query is not built", query)
    assertTrue("Query type is wrong", query.isInstanceOf[BooleanQuery])
    val booleanQuery = query.asInstanceOf[BooleanQuery]
    assertEquals("Query num clauses is wrong", 2, booleanQuery.clauses().size())
    val minClause: BooleanClause = booleanQuery.clauses().get(1)
    assertEquals("Query is wrong", BooleanClause.Occur.MUST_NOT, minClause.getOccur)
    query = minClause.getQuery()
    assertEquals("Query is wrong", classOf[IntersectsRPTVerifyQuery], query.getClass)
    val minFilter = query.asInstanceOf[IntersectsRPTVerifyQuery]
    assertEquals("Query is wrong", "IntersectsVerified(fieldName=)", minFilter.toString())
    val maxClause: BooleanClause = booleanQuery.clauses().get(0)
    assertEquals("Query is wrong", BooleanClause.Occur.FILTER, maxClause.getOccur)
    query = maxClause.getQuery()
    assertEquals("Query type is wrong", classOf[IntersectsRPTVerifyQuery], query.getClass)
    assertEquals("Query is wrong", "IntersectsVerified(fieldName=)", query.toString())
  }
}