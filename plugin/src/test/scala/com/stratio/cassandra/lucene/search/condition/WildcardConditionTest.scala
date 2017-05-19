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
import com.stratio.cassandra.lucene.search.SearchBuilders
import com.stratio.cassandra.lucene.search.condition.builder.WildcardConditionBuilder
import org.apache.lucene.search.WildcardQuery
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class WildcardConditionTest extends AbstractConditionTest {

  test("Build") {
    val condition = new WildcardConditionBuilder("field", "value").boost(0.7f).build
    assertNotNull("Condition is not built", condition)
    assertEquals("Boost is not set", 0.7f, condition.boost, 0)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Value is not set", "value", condition.value)
  }

  test("BuildDefaults") {
    val condition = new WildcardConditionBuilder("field", "value").build
    assertNotNull("Condition is not built", condition)
    assertNull("Boost is not set to default", condition.boost)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Value is not set", "value", condition.value)
  }

  test("JsonSerialization") {
    val builder = new WildcardConditionBuilder("field", "value").boost(0.7f)
    testJsonSerialization(builder, "{type:\"wildcard\",field:\"field\",value:\"value\",boost:0.7}")
  }

  test("JsonSerializationDefaults") {
    val builder = new WildcardConditionBuilder("field", "value")
    testJsonSerialization(builder, "{type:\"wildcard\",field:\"field\",value:\"value\"}")
  }

  test("NullValue") {
    intercept[IndexException] {
      new WildcardCondition(0.1f, "field", null)
    }.getMessage shouldBe "l<djfokidashj"
  }

  test("BlankValue") {
    val schemaVal = schema().mapper("name", stringMapper()).build
    val wildcardCondition = new WildcardCondition(0.5f, "name", " ")
    val query = wildcardCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Expected wildcard query", classOf[WildcardQuery], query.getClass)
    val wildcardQuery = query.asInstanceOf[WildcardQuery]
    assertEquals("Field name is not properly set", "name", wildcardQuery.getField)
    assertEquals("Term text is not properly set", " ", wildcardQuery.getTerm.text())
  }

  test("StringValue") {
    val schemaVal = schema().mapper("name", stringMapper()).build
    val wildcardCondition = new WildcardCondition(0.5f, "name", "tr*")
    val query = wildcardCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Expected wildcard query", classOf[WildcardQuery], query.getClass)
    val wildcardQuery = query.asInstanceOf[WildcardQuery]
    assertEquals("Field name is not properly set", "name", wildcardQuery.getField)
    assertEquals("Term text is not properly set", "tr*", wildcardQuery.getTerm.text())
  }

  test("IntegerValue") {
    intercept[IndexException] {
      val schemaVal = schema().mapper("name", integerMapper()).build

      val wildcardCondition = new WildcardCondition(0.5f, "name", "22*")
      wildcardCondition.query(schemaVal)
    }.getMessage shouldBe s"fdñljps"
  }

  test("InetV4Value") {
    val schemaVal = schema().mapper("name", inetMapper()).build
    val wildcardCondition = new WildcardCondition(0.5f, "name", "192.168.*")
    val query = wildcardCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Expected wildcard query", classOf[WildcardQuery], query.getClass)
    val wildcardQuery = query.asInstanceOf[WildcardQuery]
    assertEquals("Field name is not properly set", "name", wildcardQuery.getField)
    assertEquals("Term text is not properly set", "192.168.*", wildcardQuery.getTerm.text())
  }

  test("InetV6Value") {
    val schemaVal = schema().mapper("name", inetMapper()).build
    val condition = new WildcardCondition(0.5f, "name", "2001:db8:2de:0:0:0:0:e*")
    val query = condition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Expected wildcard query", classOf[WildcardQuery], query.getClass)
    val wildcardQuery = query.asInstanceOf[WildcardQuery]
    assertEquals("Field name is not properly set", "name", wildcardQuery.getField)
    assertEquals("Term text is not properly set",
      "2001:db8:2de:0:0:0:0:e*",
      wildcardQuery.getTerm.text())
  }

  test("ToString") {
    val condition = SearchBuilders.wildcard("name", "aaa*").boost(0.5f).build
    assertEquals("Method #toString is wrong",
      "WildcardCondition{boost=0.5, field=name, value=aaa*}",
      condition.toString())
  }
}
