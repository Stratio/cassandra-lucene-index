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
import com.stratio.cassandra.lucene.schema.SchemaBuilders.{schema, textMapper}
import com.stratio.cassandra.lucene.search.condition.builder.PhraseConditionBuilder
import org.apache.lucene.search.PhraseQuery
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class PhraseConditionTest extends AbstractConditionTest {

  test("Build") {
    val condition = new PhraseConditionBuilder("field", "value1 value2").slop(2).boost(0.7f).build
    assertNotNull("Condition is not built", condition)
    assertEquals("Boost is not set", 0.7f, condition.boost, 0)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Value is not set", "value1 value2", condition.value)
    assertEquals("Slop is not set", 2, condition.slop)
  }

  test("BuildDefaults") {
    val condition = new PhraseConditionBuilder("field", "value1 value2").build
    assertNotNull("Condition is not built", condition)
    assertNull("Boost is not set to default", condition.boost)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Value is not set", "value1 value2", condition.value)
    assertEquals("Slop is not set to default", PhraseCondition.DEFAULT_SLOP, condition.slop)
  }

  test("BuildNullValues") {
    intercept[IndexException] {
      new PhraseConditionBuilder("field", null).build
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildNegativeSlop") {
    intercept[IndexException] {
      new PhraseConditionBuilder("field", "value1 value2").slop(-1).build
    }.getMessage shouldBe s"fdñljps"
  }

  test("JsonSerialization") {
    val builder = new PhraseConditionBuilder("field", "value1 value2").slop(2).boost(0.7f)
    testJsonSerialization(builder,
      "{type:\"phrase\",field:\"field\",value:\"value1 value2\",boost:0.7,slop:2}")
  }

  test("JsonSerializationDefaults") {
    val builder = new PhraseConditionBuilder("field", "value1 value2")
    testJsonSerialization(builder, "{type:\"phrase\",field:\"field\",value:\"value1 value2\"}")
  }

  test("PhraseQuery") {
    val schemaVal = schema().mapper("name", textMapper().analyzer("spanish")).build

    val value = "hola adios  the    a"
    val condition = new PhraseCondition(0.5f, "name", value, 2)
    val query = condition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[PhraseQuery], query.getClass)

    val luceneQuery = query.asInstanceOf[PhraseQuery]
    assertEquals("Query terms are wrong", 3, luceneQuery.getTerms.length)
    assertEquals("Query slop is wrong", 2, luceneQuery.getSlop)
  }

  test("ToString") {
    val condition = new PhraseCondition(0.5f, "name", "hola adios", 2)
    assertEquals("Method #toString is wrong",
      "PhraseCondition{boost=0.5, field=name, value=hola adios, slop=2}",
      condition.toString())
  }
}
