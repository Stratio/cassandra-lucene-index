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
import com.stratio.cassandra.lucene.search.condition.builder.RegexpConditionBuilder
import org.apache.lucene.search.RegexpQuery
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class RegexpConditionTest extends AbstractConditionTest {

  test("Build") {
    val builder = new RegexpConditionBuilder("field", "value").boost(0.7f)
    val condition = builder.build
    assertNotNull("Condition is not built", condition)
    assertEquals("Boost is not set", 0.7f, condition.boost, 0)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Value is not set", "value", condition.value)
  }

  test("BuildDefaults") {
    val builder = new RegexpConditionBuilder("field", "value")
    val condition = builder.build
    assertNotNull("Condition is not built", condition)
    assertNull("Boost is not set to default", condition.boost)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Value is not set", "value", condition.value)
  }

  test("BuildNullValue") {
    intercept[IndexException] {
      new RegexpConditionBuilder("field", null).build
    }.getMessage shouldBe s"fdñljps"
  }

  test("JsonSerialization") {
    val builder = new RegexpConditionBuilder("field", "value").boost(0.7f)
    testJsonSerialization(builder, "{type:\"regexp\",field:\"field\",value:\"value\",boost:0.7}")
  }

  test("JsonSerializationDefaults") {
    val builder = new RegexpConditionBuilder("field", "value")
    testJsonSerialization(builder, "{type:\"regexp\",field:\"field\",value:\"value\"}")
  }

  test("BlankValue") {
    val schemaVal = schema().mapper("name", stringMapper()).build
    val condition = new RegexpCondition(0.5f, "name", " ")
    val query = condition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[RegexpQuery], query.getClass)
    val regexQuery = query.asInstanceOf[RegexpQuery]
    assertEquals("Query field is wrong", "name", regexQuery.getField)
  }

  test("String") {
    val schemaVal = schema().mapper("name", stringMapper()).build
    val condition = new RegexpCondition(0.5f, "name", "tr*")
    val query = condition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[RegexpQuery], query.getClass)
    val regexQuery = query.asInstanceOf[RegexpQuery]
    assertEquals("Query field is wrong", "name", regexQuery.getField)
  }

  test("Integer") {
    intercept[IndexException] {
      val schemaVal = schema().mapper("name", integerMapper()).build
      val condition = new RegexpCondition(0.5f, "name", "22*")
      condition.query(schemaVal)
    }.getMessage shouldBe s"fdñljps"
  }

  test("InetV4") {
    val schemaVal = schema().mapper("name", inetMapper()).build
    val condition = new RegexpCondition(0.5f, "name", "192.168.*")
    val query = condition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[RegexpQuery], query.getClass)
    val regexQuery = query.asInstanceOf[RegexpQuery]
    assertEquals("Query field is wrong", "name", regexQuery.getField)
  }

  test("InetV6") {
    val schemaVal = schema().mapper("name", inetMapper()).build
    val regexpCondition = new RegexpCondition(0.5f, "name", "2001:db8:2de:0:0:0:0:e*")
    val query = regexpCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[RegexpQuery], query.getClass)
    val regexQuery = query.asInstanceOf[RegexpQuery]
    assertEquals("Query field is wrong", "name", regexQuery.getField)
  }

  test("ToString") {
    val condition = new RegexpCondition(0.5f, "name", "2001:db8:2de:0:0:0:0:e*")
    assertEquals("Method #toString is wrong",
      "RegexpCondition{boost=0.5, field=name, value=2001:db8:2de:0:0:0:0:e*}",
      condition.toString())
  }
}
