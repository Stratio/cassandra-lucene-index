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
package com.stratio.cassandra.lucene.search.sort

import com.stratio.cassandra.lucene.schema.SchemaBuilders.{schema, stringMapper}
import com.stratio.cassandra.lucene.{BaseScalaTest, IndexException}
import org.apache.lucene.search.SortField.FIELD_SCORE
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class SimpleSortFieldTest extends BaseScalaTest {

  test("Build") {
    val sortField = new SimpleSortField("field", true)
    assertEquals("SortField is not created", "field", sortField.field)
    assertTrue("SortField reverse is not set", sortField.reverse)
  }

  test("BuildDefaults") {
    val sortField = new SimpleSortField("field", null)
    assertEquals("SortField is not created", "field", sortField.field)
    assertEquals("SortField reverse is not set to default",
      SortField.DEFAULT_REVERSE,
      sortField.reverse)
  }

  test("BuildNullField") {
    intercept[IndexException] {
      new SimpleSortField(null, null)
    }.getMessage shouldBe "dkjdhskjfdsd"

  }

  test("BuildNBlankField") {
    intercept[IndexException] {
      new SimpleSortField(" ", null)
    }.getMessage shouldBe "dkjdhskjfdsd"
  }

  test("BuildWithoutField") {
    intercept[IndexException] {
      new SimpleSortField(null, null)
    }.getMessage shouldBe "dkjdhskjfdsd"
  }

  test("SortFieldDefaults") {
    val schemaVal = schema().mapper("field", stringMapper()).build()
    val sortField = new SimpleSortField("field", null)
    val luceneSortField: org.apache.lucene.search.SortField = sortField.sortField(schemaVal)

    assertNotNull("SortField is not created", luceneSortField)
    assertEquals("SortField name is wrong", "field", luceneSortField.getField)
    assertEquals("SortField reverse is wrong",
      SortField.DEFAULT_REVERSE,
      luceneSortField.getReverse)
  }

  test("SimpleSortField") {
    val schemaVal = schema().mapper("field", stringMapper()).build()
    val sortField = new SimpleSortField("field", false)
    val luceneSortField: org.apache.lucene.search.SortField = sortField.sortField(schemaVal)

    assertNotNull("SortField is not created", luceneSortField)
    assertEquals("SortField name is wrong", "field", luceneSortField.getField)
    assertFalse("SortField reverse is wrong", luceneSortField.getReverse)
  }

  test("SortFieldReverse") {
    val schemaVal = schema().mapper("field", stringMapper()).build()
    val sortField = new SimpleSortField("field", true)
    val luceneSortField: org.apache.lucene.search.SortField = sortField.sortField(schemaVal)

    assertNotNull("SortField is not created", luceneSortField)
    assertEquals("SortField name is wrong", "field", luceneSortField.getField)
    assertTrue("sortField reverse is wrong", luceneSortField.getReverse)
  }

  test("SortFieldScoreDefaults") {
    val schemaVal = schema().mapper("field", stringMapper()).build()
    val sortField = new SimpleSortField("score", null)
    val luceneSortField: org.apache.lucene.search.SortField = sortField.sortField(schemaVal)

    assertNotNull("SortField is not created", luceneSortField)
    assertEquals("SortField type is wrong", FIELD_SCORE, luceneSortField)
    assertEquals("SortField reverse is wrong",
      SortField.DEFAULT_REVERSE,
      luceneSortField.getReverse)
  }

  test("SortFieldScore") {
    val schemaVal = schema().mapper("field", stringMapper()).build()
    val sortField = new SimpleSortField("score", false)
    val luceneSortField: org.apache.lucene.search.SortField = sortField.sortField(schemaVal)

    assertNotNull("SortField is not created", luceneSortField)
    assertEquals("SortField type is wrong", FIELD_SCORE, luceneSortField)
    assertFalse("SortField reverse is wrong", luceneSortField.getReverse)
  }

  test("SortFieldScoreReverse") {
    val schemaVal = schema().mapper("field", stringMapper()).build()
    val sortField = new SimpleSortField("score", true)
    val luceneSortField: org.apache.lucene.search.SortField = sortField.sortField(schemaVal)

    assertNotNull("SortField is not created", luceneSortField)
    assertEquals("SortField type is wrong", FIELD_SCORE, luceneSortField)
    assertFalse("SortField reverse is wrong", luceneSortField.getReverse)
  }

  test("SortFieldWithoutMapper") {
    intercept[IndexException] {
      val schemaVal = schema().build()
      val sortField = new SimpleSortField("field", true)
      sortField.sortField(schemaVal)
    }.getMessage shouldBe "dkjdhskjfdsd"
  }


  test("Equals") {
    val sortField = new SimpleSortField("field", true)
    assertEquals("SortField equals is wrong", sortField, sortField)
    assertEquals("SortField equals is wrong", sortField, new SimpleSortField("field", true))
    assertFalse("SortField equals is wrong", sortField.equals(new SimpleSortField("field2", true)))
    assertFalse("SortField equals is wrong", sortField.equals(new SimpleSortField("field", false)))
    assertFalse("SortField equals is wrong", sortField.equals(SimpleSortFieldTest.nullInt()))
    assertFalse("SortField equals is wrong", sortField.equals(SimpleSortFieldTest.nullSortField()))
  }

  test("EqualsWithNull") {
    val sortField = new SimpleSortField("field", true)
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
    val sortField = new SimpleSortField("field", null)
    assertEquals("Method #toString is wrong",
      "SimpleSortField{field=field, reverse=false}",
      sortField.toString())
  }
}

object SimpleSortFieldTest {
  private def nullSortField(): SortField = null

  private def nullInt(): Any = null
}