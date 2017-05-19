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

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.schema.SchemaBuilders.{schema, stringMapper}
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper
import com.stratio.cassandra.lucene.{BaseScalaTest, IndexException}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.search.{MatchAllDocsQuery, Query}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class SingleColumnConditionTest extends BaseScalaTest {

  test("Build") {
    val condition = new SingleColumnConditionTest.MockCondition(0.5f, "field")
    assertEquals("Boost is not properly set", 0.5f, condition.boost_, 0)
    assertEquals("Field name is not properly set", "field", condition.field_)
  }

  test("BuildDefaults") {
    val condition = new SingleColumnConditionTest.MockCondition(null, "field")
    assertNull("Boost is not set to default", condition.boost_)
  }

  test("BuildNullField") {
    intercept[IndexException] {
      new SingleColumnConditionTest.MockCondition(null, null)
    }.getMessage shouldBe s"fdñljps"
  }

  test("BuildBlankField") {
    intercept[IndexException] {
      new SingleColumnConditionTest.MockCondition(null, " ")
    }.getMessage shouldBe s"fdñljps"
  }

  test("GetMapper") {
    val schemaVal = schema().mapper("field", stringMapper()).build
    val condition = new SingleColumnConditionTest.MockCondition(null, "field")
    val query = condition.query(schemaVal)
    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[MatchAllDocsQuery], query.getClass)
  }

  test("GetMapperNotFound") {
    intercept[IndexException] {
      val schemaVal = schema().build
      val condition = new SingleColumnConditionTest.MockCondition(null, "field")
      condition.query(schemaVal)
    }.getMessage shouldBe s"fdñljps"
  }
}

object SingleColumnConditionTest {

  class MockCondition(boost: Float, field: String) extends SingleColumnCondition(boost, field) {
    override def doQuery(
        mapper: SingleColumnMapper[_],
        analyzer: Analyzer): Query = new MatchAllDocsQuery()

    override def toStringHelper: MoreObjects.ToStringHelper = toStringHelper(this)
  }

}