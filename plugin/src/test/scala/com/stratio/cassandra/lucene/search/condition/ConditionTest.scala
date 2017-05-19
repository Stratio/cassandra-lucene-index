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

import java.util
import java.util.Collections

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.schema.Schema
import com.stratio.cassandra.lucene.schema.SchemaBuilders.schema
import com.stratio.cassandra.lucene.search.condition.ConditionTest.MockCondition
import org.apache.lucene.search.{BoostQuery, MatchAllDocsQuery, Query}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class ConditionTest extends BaseScalaTest {

  test("ConstructorWithBoost") {
    val condition = new MockCondition(0.7f)
    assertEquals("Query boost is wrong", 0.7f, condition.boost_, 0)

  }

  test("ConstructorWithoutBoost") {
    val condition = new MockCondition(null)
    assertNull("Query boost is wrong", condition.boost_)
  }

  test("QueryWithBoost") {
    val condition = new MockCondition(0.7f)
    val schemaVal = schema().build
    val query = condition.query(schemaVal)
    assertTrue("Query is not boosted", query.isInstanceOf[BoostQuery])
    val boostQuery = query.asInstanceOf[BoostQuery]
    assertEquals("Query boost is wrong", 0.7f, boostQuery.getBoost, 0)
  }

  test("QueryWithoutBoost") {
    val condition = new MockCondition(null)
    val schemaVal = schema().build
    val query = condition.query(schemaVal)
    assertEquals("Query type is wrong", classOf[MatchAllDocsQuery], query.getClass)
  }
}

object ConditionTest {

  class MockCondition(val _boost: Float) extends Condition(_boost) {
    override def doQuery(schema: Schema): Query = new MatchAllDocsQuery()

    def postProcessingFields: util.Set[String] = Collections.emptySet()

    override def toStringHelper: MoreObjects.ToStringHelper = toStringHelper(this)
  }

}