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
import com.stratio.cassandra.lucene.search.SearchBuilders.contains
import com.stratio.cassandra.lucene.util.ByteBufferUtils
import org.apache.lucene.search.{BooleanClause, BooleanQuery, TermQuery}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class ContainsConditionTest extends AbstractConditionTest {

  test("BuildDefaults") {
    val values: Array[Any] = Array[Any]("a", "b")
    val condition = contains("field", values).build
    assertNotNull("Condition is not built", condition)
    assertNull("Boost is not set to default", condition.boost)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Values is not set", values, condition.values)
    assertEquals("Doc values is not set", MatchCondition.DEFAULT_DOC_VALUES, condition.docValues)
  }

  test("BuildStrings") {
    val values: Array[Any] = Array[Any]("a", "b")
    val condition = contains("field", values).boost(0.7f).docValues(true).build
    assertNotNull("Condition is not built", condition)
    assertEquals("Boost is not set", 0.7f, condition.boost, 0)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Values is not set",
      values.asInstanceOf[Object],
      condition.values.asInstanceOf[Array[Object]])
    assertTrue("Doc values is not set", condition.docValues)
  }

  test("BuildNumbers") {
    val values: Array[Any] = Array[Any](1, 2, -3)
    val condition = contains("field", values).boost(0.7f).docValues(true).build
    assertNotNull("Condition is not built", condition)
    assertEquals("Boost is not set", 0.7f, condition.boost, 0)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Values is not set", values, condition.values)
    assertTrue("Doc values is not set", condition.docValues)
  }

  test("BuildWithNullField") {
    intercept[IndexException] {
      contains(null, Array(1, 2, 3)).build
    }.getMessage shouldBe s"ñlkjofijw"
  }

  test("BuildWithBlankField") {
    intercept[IndexException] {
      contains(" ", Array(1, 2, 3)).build
    }.getMessage shouldBe s"ñlkjofijw"
  }

  test("BuildWithNullValues") {
    intercept[IndexException] {
      contains("values", Array()).build
    }.getMessage shouldBe s"ñlkjofijw"
  }

  test("JsonSerializationStrings") {

    val builder = contains("field", Array("a", "b")).boost(0.7f).docValues(true)
    testJsonSerialization(builder,
      "{type:\"contains\",field:\"field\",values:[\"a\",\"b\"],boost:0.7,doc_values:true}")
  }

  test("JsonSerializationNumbers") {
    val builder = contains("field", Array(1, 2, -3)).boost(0.7f).docValues(true)
    testJsonSerialization(builder,
      "{type:\"contains\",field:\"field\",values:[1,2,-3],boost:0.7,doc_values:true}")
  }

  test("QueryNumeric") {
    val values: Array[Any] = Array[Any](0, 1, 2)
    val schemaVal = schema().mapper("name", integerMapper()).build

    val condition = contains("name", values).build
    val query = condition.doQuery(schemaVal)
    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[BooleanQuery], query.getClass)

    val booleanQuery = query.asInstanceOf[BooleanQuery]
    val clauses: java.util.List[BooleanClause] = booleanQuery.clauses
    assertEquals("Query is wrong", values.length, clauses.size())
    val query0: TermQuery = clauses.get(0).getQuery.asInstanceOf[TermQuery]
    val query1: TermQuery = clauses.get(1).getQuery.asInstanceOf[TermQuery]
    val query2: TermQuery = clauses.get(2).getQuery.asInstanceOf[TermQuery]
    assertEquals("Query value is wrong",
      "600800000000",
      ByteBufferUtils.toHex(query0.getTerm.bytes()))
    assertEquals("Query value is wrong",
      "600800000001",
      ByteBufferUtils.toHex(query1.getTerm.bytes()))
    assertEquals("Query value is wrong",
      "600800000002",
      ByteBufferUtils.toHex(query2.getTerm.bytes()))
  }

  test("QueryString") {
    val values: Array[Any] = Array[Any]("houses", "cats")
    val schemaVal = schema().mapper("name", stringMapper()).build

    val condition = contains("name", values).build
    val query = condition.doQuery(schemaVal)
    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[BooleanQuery], query.getClass)

    val booleanQuery = query.asInstanceOf[BooleanQuery]
    val clauses: java.util.List[BooleanClause] = booleanQuery.clauses
    val query1 = clauses.get(0).getQuery.asInstanceOf[TermQuery]
    val query2 = clauses.get(1).getQuery.asInstanceOf[TermQuery]
    assertEquals("Query is wrong", "houses", query1.getTerm.bytes().utf8ToString())
    assertEquals("Query is wrong", "cats", query2.getTerm.bytes().utf8ToString())
  }

  test("QueryText") {
    val values: Array[Any] = Array[Any]("houses", "cats")
    val schemaVal = schema().mapper("name", textMapper()).defaultAnalyzer("english").build
    val condition = contains("name", values).build
    val query = condition.doQuery(schemaVal)
    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[BooleanQuery], query.getClass)

    val booleanQuery = query.asInstanceOf[BooleanQuery]
    val clauses: java.util.List[BooleanClause] = booleanQuery.clauses
    val termQuery1 = clauses.get(0).getQuery.asInstanceOf[TermQuery]
    val termQuery2 = clauses.get(1).getQuery.asInstanceOf[TermQuery]
    assertEquals("Query is wrong", "hous", termQuery1.getTerm.bytes().utf8ToString())
    assertEquals("Query is wrong", "cat", termQuery2.getTerm.bytes().utf8ToString())
  }

  test("ToString") {
    val condition = contains("field", Array("value1", "value2")).boost(0.7f).docValues(true).build
    assertEquals("Method #toString is wrong",
      "ContainsCondition{boost=0.7, field=field, values=[value1, value2], docValues=true}",
      condition.toString())
  }
}
