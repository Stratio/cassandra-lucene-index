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
package com.stratio.cassandra.lucene.search.condition

import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.schema.SchemaBuilders._
import com.stratio.cassandra.lucene.search.SearchBuilders.bitemporal
import com.stratio.cassandra.lucene.search.condition.builder.BitemporalConditionBuilder
import org.apache.lucene.search.BooleanQuery
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Eduardo Alonso  `eduardoalonso@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class BitemporalConditionTest extends AbstractConditionTest {

  test("BuildLong") {
    val condition = new BitemporalConditionBuilder("field").boost(0.7f).ttFrom(1L).ttTo(2L)
      .vtFrom(3L).vtTo(4L).build
    assertNotNull("Condition is not built", condition)
    assertEquals("Boost is not set", 0.7f, condition.boost, 0)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("tt_from is not set", 1l, condition.ttFrom)
    assertEquals("tt_to is not set", 2l, condition.ttTo)
    assertEquals("vt_from is not set", 3l, condition.vtFrom)
    assertEquals("vt_to is not set", 4l, condition.vtTo)
  }

  test("BuildString") {
    val condition = new BitemporalConditionBuilder("field").boost(0.7f)
      .ttFrom("2015/03/20 11:45:32.333")
      .ttTo("2013/03/20 11:45:32.333")
      .vtFrom("2012/03/20 11:45:32.333")
      .vtTo("2011/03/20 11:45:32.333")
      .build
    assertNotNull("Condition is not built", condition)
    assertEquals("Boost is not set", 0.7f, condition.boost, 0)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("tt_from is not set", "2015/03/20 11:45:32.333", condition.ttFrom)
    assertEquals("tt_to is not set", "2013/03/20 11:45:32.333", condition.ttTo)
    assertEquals("vt_from is not set", "2012/03/20 11:45:32.333", condition.vtFrom)
    assertEquals("vt_to is not set", "2011/03/20 11:45:32.333", condition.vtTo)
  }

  test("BuildDefaults") {
    val condition = new BitemporalConditionBuilder("field").build
    assertNotNull("Condition is not built", condition)
    assertNull("Boost is not set to default", condition.boost)
    assertEquals("Field is not set", "field", condition.field)
    assertNull("tt_from is not set to default", condition.ttFrom)
    assertNull("tt_to is not set to default", condition.ttTo)
    assertNull("vt_from is not set to default", condition.vtFrom)
    assertNull("vt_to is not set to default", condition.vtTo)
  }

  test("JsonSerialization") {
    val builder = new BitemporalConditionBuilder("field").boost(0.7f)
    testJsonSerialization(builder, "{type:\"bitemporal\",field:\"field\",boost:0.7}")
  }

  test("JsonSerializationDefaults") {
    val builder = new BitemporalConditionBuilder("field")
    testJsonSerialization(builder, "{type:\"bitemporal\",field:\"field\"}")
  }

  test("Query") {

    val mapperBuilder = bitemporalMapper("vtFrom",
      "vtTo",
      "ttFrom",
      "ttTo").pattern("yyyy")
    val schemaVal = schema().mapper("name", mapperBuilder).build
    val condition = new BitemporalCondition(0.5f, "name", 2001, 2002, 2003, 2004)

    val query = condition.doQuery(schemaVal)
    assertNotNull("Query is not built", query)
    assertTrue("Query type is wrong", query.isInstanceOf[BooleanQuery])
  }

  test("QueryWithoutValidMapper") {
    intercept[IndexException] {
      val schemaVal = schema().mapper("name", uuidMapper()).build
      val condition = new BitemporalCondition(null, "name", 1, 2, 3, 4)
      condition.query(schemaVal)
    }.getMessage shouldBe s""
  }


  test("ToString") {
    val condition = bitemporal("name").vtFrom(1).vtTo(2).ttFrom(3).ttTo(4).boost(0.3f).build
    assertEquals("Method #toString is wrong",
      "BitemporalCondition{boost=0.3, field=name, vtFrom=1, vtTo=2, ttFrom=3, ttTo=4}",
      condition.toString)
  }
}
