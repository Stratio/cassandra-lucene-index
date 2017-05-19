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

import com.stratio.cassandra.lucene.schema.SchemaBuilders.schema
import com.stratio.cassandra.lucene.search.condition.builder.AllConditionBuilder
import org.apache.lucene.search.MatchAllDocsQuery
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class AllConditionTest extends AbstractConditionTest {

  test("Build") {
    val builder = new AllConditionBuilder().boost(0.7f)
    val condition = builder.build
    assertNotNull("Condition is not built", condition)
    assertEquals("Boost is not set", 0.7f, condition.boost, 0)
  }

  test("BuildDefaults") {
    val builder = new AllConditionBuilder()
    val condition = builder.build
    assertNotNull("Condition is not built", condition)
    assertNull("Boost is not set to default", condition.boost)
  }

  test("JsonSerialization") {
    val builder = new AllConditionBuilder().boost(0.7f)
    testJsonSerialization(builder, "{type:\"all\",boost:0.7}")
  }

  test("JsonSerializationDefaults") {
    val builder = new AllConditionBuilder()
    testJsonSerialization(builder, "{type:\"all\"}")
  }

  test("Query") {
    val condition = new AllCondition(0.7f)
    val schemaVal = schema().build
    val query = condition.doQuery(schemaVal)
    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[MatchAllDocsQuery], query.getClass)
  }

  test("ToString") {
    val condition = new AllCondition(0.7f)
    assertEquals("Method #toString is wrong", "AllCondition{boost=0.7}", condition.toString())
  }
}
