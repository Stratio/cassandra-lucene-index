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
package com.stratio.cassandra.lucene.search.sort

import com.stratio.cassandra.lucene.schema.SchemaBuilders.{geoPointMapper, schema}
import com.stratio.cassandra.lucene.{BaseScalaTest, IndexException}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Eduardo Alonso `eduardoalonso@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class GeoDistanceSortFieldTest extends BaseScalaTest {
  test("Build") {
    val sortField = new GeoDistanceSortField("geo_place", true, 0.0, 0.0)
    assertEquals("SortField field name is not set", "geo_place", sortField.field)
    assertTrue("SortField reverse is not set", sortField.reverse)
    assertTrue("SortField longitude is not set", sortField.longitude == 0.0)
    assertTrue("SortField latitude is not set", sortField.latitude == 0.0)
  }

  test("BuildDefaults") {
    val sortField = new GeoDistanceSortField("geo_place", null, 0.0, 0.0)
    assertEquals("SortField field name is not set", "geo_place", sortField.field)
    assertEquals("SortField reverse is not set to default",
      SortField.DEFAULT_REVERSE,
      sortField.reverse)
  }

  test("BuildNullField") {
    intercept[IndexException] {
      new GeoDistanceSortField(null, null, 0.0, 0.0)
    }.getMessage shouldBe s"db,gkdpñfkgd"
  }

  test("BuildNBlankField") {
    intercept[IndexException] {
      new GeoDistanceSortField(" ", null, 0.0, 0.0)
    }.getMessage shouldBe s"db,gkdpñfkgd"
  }

  test("BuildWithoutField") {
    intercept[IndexException] {
      new GeoDistanceSortField(null, null, 0.0, 0.0)
    }.getMessage shouldBe s"db,gkdpñfkgd"
  }

  test("GeoDistanceSortFieldDefaults") {
    val schemaVal = schema().mapper("field", geoPointMapper("latitude", "longitude")).build()
    val sortField = new GeoDistanceSortField("field", null, 0.0, 0.0)
    val luceneSortField = sortField.sortField(schemaVal)

    assertNotNull("SortField is not created", luceneSortField)
    assertEquals("SortField reverse is wrong",
      SortField.DEFAULT_REVERSE,
      luceneSortField.getReverse)
    assertEquals("SortField type is wrong",
      luceneSortField.getType,
      org.apache.lucene.search.SortField.Type.REWRITEABLE)
  }

  test("GeoDistanceSortField") {
    val schemaVal = schema().mapper("field", geoPointMapper("latitude", "longitude")).build()
    val sortField = new GeoDistanceSortField("field", false, 0.0, 0.0)
    val luceneSortField = sortField.sortField(schemaVal)

    assertNotNull("SortField is not created", luceneSortField)
    assertFalse("SortField reverse is wrong", luceneSortField.getReverse())
  }

  test("GeoDistanceSortFieldReverse") {
    val schemaVal = schema().mapper("field", geoPointMapper("latitude", "longitude")).build()
    val sortField = new GeoDistanceSortField("field", true, 0.0, 0.0)
    val luceneSortField = sortField.sortField(schemaVal)

    assertNotNull("SortField is not created", luceneSortField)
    assertTrue("sortField reverse is wrong", luceneSortField.getReverse)
  }

  test("GeoDistanceSortFieldWithoutMapper") {
    intercept[IndexException] {
      val schemaVal = schema().build
      val sortField = new GeoDistanceSortField("field", false, 0.0, 0.0)
      sortField.sortField(schemaVal)
    }.getMessage shouldBe s"db,gkdpñfkgd"
  }

  test("Equals") {
    val sortField = new GeoDistanceSortField("field", true, 0.0, 0.0)
    assertEquals("SortField equals is wrong", sortField, sortField)
    assertEquals("SortField equals is wrong",
      sortField,
      new GeoDistanceSortField("field", true, 0.0, 0.0))
    assertFalse("SortField equals is wrong",
      sortField.equals(new GeoDistanceSortField("field2", true, 0.0, 0.0)))
    assertFalse("SortField equals is wrong",
      sortField.equals(new GeoDistanceSortField("field", false, 0.0, 0.0)))
    assertFalse("SortField equals is wrong",
      sortField.equals(new GeoDistanceSortField("field", true, 0.0, 1.0)))
    assertFalse("SortField equals is wrong",
      sortField.equals(new GeoDistanceSortField("field", true, -1.0, 0.0)))
    assertFalse("SortField equals is wrong", sortField.equals(GeoDistanceSortFieldTest.nullInt()))
    assertFalse("SortField equals is wrong",
      sortField.equals(GeoDistanceSortFieldTest.nullSortField()))
  }

  test("EqualsWithNull") {
    val sortField = new GeoDistanceSortField("field", true, 0.0, 0.0)
    assertFalse("SortField equals is wrong", sortField.equals(null))
    assertFalse("SortField equals is wrong", sortField.equals(new Integer(0)))
  }

  test("HashCode") {
    assertEquals("SortField equals is wrong",
      -1274708409,
      new SimpleSortField("field", true).hashCode())
    assertEquals("SortField equals is wrong",
      -1274708410,
      new SimpleSortField("field", false).hashCode())
  }

  test("ToString") {
    val sortField = new GeoDistanceSortField("field", true, 0.0, 0.0)
    assertEquals("Method #toString is wrong",
      "GeoDistanceSortField{field=field, reverse=true, latitude=0.0, longitude=0.0}",
      sortField.toString())
  }
}

object GeoDistanceSortFieldTest {
  def nullSortField(): SortField = null
  def nullInt(): Any = null
}
