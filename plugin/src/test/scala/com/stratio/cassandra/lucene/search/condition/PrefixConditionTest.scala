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

import com.stratio.cassandra.lucene.schema.SchemaBuilders._
import com.stratio.cassandra.lucene.search.condition.builder.PrefixConditionBuilder
import org.apache.lucene.search.PrefixQuery
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class PrefixConditionTest extends AbstractConditionTest {

  test("Build") {
    val condition = new PrefixConditionBuilder("field", "value").boost(0.7f).build
    assertNotNull("Condition is not built", condition)
    assertEquals("Boost is not set", 0.7f, condition.boost, 0)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Value is not set", "value", condition.value)
  }

  test("BuildDefaults") {
    val condition = new PrefixConditionBuilder("field", "value").build
    assertNotNull("Condition is not built", condition)
    assertNull("Boost is not set to default", condition.boost)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Value is not set", "value", condition.value)
  }

  test("JsonSerialization") {
    val builder = new PrefixConditionBuilder("field", "value").boost(0.7f)
    testJsonSerialization(builder, "{type:\"prefix\",field:\"field\",value:\"value\",boost:0.7}")
  }

  test("JsonSerializationDefaults") {
    val builder = new PrefixConditionBuilder("field", "value")
    testJsonSerialization(builder, "{type:\"prefix\",field:\"field\",value:\"value\"}")
  }

  test("StringValue") {
    val schemaVal = schema().mapper("name", stringMapper()).build

    val prefixCondition = new PrefixCondition(0.5f, "name", "tr")
    val query = prefixCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[PrefixQuery], query.getClass)

    val luceneQuery = query.asInstanceOf[PrefixQuery]
    assertEquals("Query field is wrong", "name", luceneQuery.getField)
    assertEquals("Query prefix is wrong", "tr", luceneQuery.getPrefix.text())
  }

  test("InetV4Value") {
    val schemaVal = schema().mapper("name", inetMapper()).build

    val wildcardCondition = new PrefixCondition(0.5f, "name", "192.168.")
    val query = wildcardCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", PrefixQuery.class, query.getClass)

    val luceneQuery = query.asInstanceOf[PrefixQuery]
    assertEquals("Query field is wrong", "name", luceneQuery.getField)
    assertEquals("Query prefix is wrong", "192.168.", luceneQuery.getPrefix.text())
  }

  test("InetV6Value") {
    val schemaVal = schema().mapper("name", inetMapper()).build

    val wildcardCondition = new PrefixCondition(0.5f, "name", "2001:db8:2de:0:0:0:0:e")
    val query = wildcardCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", PrefixQuery.class, query.getClass)

    val luceneQuery = query.asInstanceOf[PrefixQuery]
    assertEquals("Query field is wrong", "name", luceneQuery.getField)
    assertEquals("Query prefix is wrong", "2001:db8:2de:0:0:0:0:e", luceneQuery.getPrefix.text())
  }

  test("ToString") {
    val condition = new PrefixCondition(0.5f, "name", "tr")
    assertEquals("Method #toString is wrong",
      "PrefixCondition{boost=0.5, field=name, value=tr}",
      condition.toString())
  }
}
