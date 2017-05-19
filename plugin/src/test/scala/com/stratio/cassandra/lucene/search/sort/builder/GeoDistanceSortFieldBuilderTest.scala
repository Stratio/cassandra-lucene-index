/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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
package com.stratio.cassandra.lucene.search.sort.builder

import java.io.IOException

import com.stratio.cassandra.lucene.common.JsonSerializer
import com.stratio.cassandra.lucene.search.sort.SortField
import com.stratio.cassandra.lucene.{BaseScalaTest, IndexException}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * Class for testing [[GeoDistanceSortFieldBuilder]].
  *
  * @author Eduardo Alonso `eduardoalonso@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class GeoDistanceSortFieldBuilderTest extends BaseScalaTest {

  test("Build") {
    val mapper = "geo_place"
    val latitude = 0.0
    val longitude = 0.0

    val field = new GeoDistanceSortFieldBuilder(mapper, latitude, longitude).reverse(true).build
    assertNotNull("GeoDistanceSortField is not built", field)
    assertEquals("GeoDistanceSortField field name is not set", mapper, field.field)
    assertEquals("GeoDistanceSortField reverse is not set", true, field.reverse)
    assertTrue("GeoDistanceSortField latitude is not set", latitude == field.latitude)
    assertTrue("GeoDistanceSortField longitude is not set", longitude == field.longitude)
  }

  test("BuildDefault") {
    val mapper = "geo_place"
    val latitude = 0.0
    val longitude = 0.0

    val field = new GeoDistanceSortFieldBuilder(mapper, latitude, longitude).build
    assertNotNull("GeoDistanceSortField is not built", field)
    assertEquals("GeoDistanceSortField field name is not set", mapper, field.field)
    assertEquals("GeoDistanceSortField reverse is not properly set",
      SortField.DEFAULT_REVERSE,
      field.reverse)
  }

  test("BuildInvalidLat") {
    val field = "field"
    val latitude = 91.0
    val longitude = 0.0

    val builder = new GeoDistanceSortFieldBuilder(field, latitude, longitude)
    intercept[IndexException] {
      builder.build
    }.getMessage shouldBe "Creating a GeoDistanceSortFieldBuilder with invalid longitude must throw an IndexException, latitude must be in range [-90.0, 90.0], but found 91.0"
  }

  test("BuildInvalidLong") {
    val field = "field"
    val latitude = 0.0
    val longitude = 200.0

    val builder = new GeoDistanceSortFieldBuilder(field, latitude, longitude)
    intercept[IndexException] {
      builder.build
    }.getMessage shouldBe "Creating a GeoDistanceSortFieldBuilder with invalid longitude must throw an IndexException, longitude must be in range [-180.0, 180.0], but found 200.0"
  }

  test("BuildReverse") {
    val mapper = "geo_place"
    val field = new GeoDistanceSortFieldBuilder(mapper, 0.0, 0.0).reverse(false).build
    assertNotNull("GeoDistanceSortField is not built", field)
    assertEquals("GeoDistanceSortField field name is not set", mapper, field.field)
    assertEquals("GeoDistanceSortField reverse is not set", false, field.reverse)
  }

  test("Json") {
    intercept[IOException] {
      val json1 = "{type:\"geo_distance\",field:\"geo_place\",latitude:0.0,longitude:0.0,reverse:false}"
      val json2 = JsonSerializer.toString(JsonSerializer.fromString(json1,
        classOf[SortFieldBuilder]))
      assertEquals("JSON serialization is wrong", json1, json2)
    }.getMessage shouldBe "flshjoas"
  }

  test("JsonDefault") {
    intercept[IOException] {
      val json1 = "{type:\"geo_distance\",field:\"geo_place\",latitude:0.0,longitude:0.0,reverse:false}"
      val builder = JsonSerializer.fromString(json1, classOf[SortFieldBuilder])
      val json2 = JsonSerializer.toString(builder)
      assertEquals("JSON serialization is wrong", json1, json2)
    }.getMessage shouldBe "flshjoas"
  }

  test("JsonReverse") {
    intercept[IOException] {
      val json1 = "{type:\"geo_distance\",field:\"geo_place\",latitude:0.0,longitude:0.0,reverse:false}"
      val json2 = JsonSerializer.toString(JsonSerializer.fromString(json1,
        classOf[SortFieldBuilder]))
      assertEquals("JSON serialization is wrong", json1, json2)
    }.getMessage shouldBe "flshjoas"
  }
}