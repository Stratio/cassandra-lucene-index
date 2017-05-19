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

import java.util.{Collections, Optional, UUID}

import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.schema.SchemaBuilders._
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper
import com.stratio.cassandra.lucene.schema.mapping.builder.MapperBuilder
import com.stratio.cassandra.lucene.search.SearchBuilders.`match`
import com.stratio.cassandra.lucene.util.ByteBufferUtils
import org.apache.lucene.document.Field
import org.apache.lucene.search.{BooleanQuery, NumericRangeQuery, TermQuery}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class MatchConditionTest extends AbstractConditionTest {

  test("BuildDefaults") {
    val condition = `match`("field", "value").build
    assertNotNull("Condition is not built", condition)
    assertNull("Boost is not set to default", condition.boost)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Value is not set", "value", condition.value)
    assertEquals("Doc values is not set", MatchCondition.DEFAULT_DOC_VALUES, condition.docValues)
  }

  test("BuildString") {
    val condition = `match`("field", "value").boost(0.7f).docValues(true).build
    assertNotNull("Condition is not built", condition)
    assertEquals("Boost is not set", 0.7f, condition.boost, 0)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Value is not set", "value", condition.value)
    assertTrue("Doc values is not set", condition.docValues)
  }

  test("BuildNumber") {
    val condition = `match`("field", 3).boost(0.7f).docValues(true).build
    assertNotNull("Condition is not built", condition)
    assertEquals("Boost is not set", 0.7f, condition.boost, 0)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Value is not set", 3, condition.value)
    assertTrue("Doc values is not set", condition.docValues)
  }

  test("BlankValue") {
    val condition = `match`("field", " ").boost(0.7f).build
    assertEquals("Boost is not set", 0.7f, condition.boost, 0)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Value is not set", " ", condition.value)
  }

  test("JsonSerializationDefaults") {
    val builder = `match`("field", "value")
    testJsonSerialization(builder, "{type:\"match\",field:\"field\",value:\"value\"}")
  }

  test("JsonSerializationString") {
    val builder = `match`("field", "value").boost(0.7f).docValues(true)
    testJsonSerialization(builder,
      "{type:\"match\",field:\"field\",value:\"value\",boost:0.7,doc_values:true}")
  }

  test("JsonSerializationNumber") {
    val builder = `match`("field", 3).boost(0.7f).docValues(true)
    testJsonSerialization(builder,
      "{type:\"match\",field:\"field\",value:3,boost:0.7,doc_values:true}")
  }

  test("String") {
    val schemaVal = schema().mapper("name", stringMapper()).build
    val matchCondition = new MatchCondition(0.5f, "name", "value", false)
    val query = matchCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[TermQuery], query.getClass)

    val termQuery = query.asInstanceOf[TermQuery]
    assertEquals("Query value is wrong", "value", termQuery.getTerm.bytes().utf8ToString())
  }

  test("StringStopwords") {
    val schemaVal = schema().mapper("name", textMapper().analyzer("english")).build

    val matchCondition = new MatchCondition(0.5f, "name", "the", false)
    val query = matchCondition.doQuery(schemaVal)
    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[BooleanQuery], query.getClass)
  }

  test("Integer") {
    val schemaVal = schema().mapper("name", integerMapper()).build

    val matchCondition = `match`("name", 42).boost(0.5f).build
    val query = matchCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[TermQuery], query.getClass)
    val term = query.asInstanceOf[TermQuery].getTerm
    assertEquals("Query value is wrong", "60080000002a", ByteBufferUtils.toHex(term.bytes()))
  }

  test("Long") {
    val schemaVal = schema().mapper("name", longMapper()).build
    val matchCondition = `match`("name", 42L).build
    val query = matchCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[TermQuery], query.getClass)
    val term = query.asInstanceOf[TermQuery].getTerm
    assertEquals("Query value is wrong",
      "200100000000000000002a",
      ByteBufferUtils.toHex(term.bytes()))
  }

  test("Float") {
    val schemaVal = schema().mapper("name", floatMapper()).build
    val matchCondition = `match`("name", 42.42F).build
    val query = matchCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[NumericRangeQuery], query.getClass)

    val numericRangeQuery = query.asInstanceOf[NumericRangeQuery]
    assertEquals("Query value is wrong", 42.42F, numericRangeQuery.getMin)
    assertEquals("Query value is wrong", 42.42F, numericRangeQuery.getMax)
    assertEquals("Query value is wrong", true, numericRangeQuery.includesMin())
    assertEquals("Query value is wrong", true, numericRangeQuery.includesMax())
  }

  test("Double") {
    val schemaVal = schema().mapper("name", doubleMapper()).build
    val matchCondition = `match`("name", 42.42D).build
    val query = matchCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[NumericRangeQuery], query.getClass)

    val numericRangeQuery = query.asInstanceOf[NumericRangeQuery]
    assertEquals("Query value is wrong", 42.42D, numericRangeQuery.getMin)
    assertEquals("Query value is wrong", 42.42D, numericRangeQuery.getMax)
    assertEquals("Query value is wrong", true, numericRangeQuery.includesMin())
    assertEquals("Query value is wrong", true, numericRangeQuery.includesMax())
  }

  test("Blob") {
    val schemaVal = schema().mapper("name", blobMapper()).build
    val matchCondition = `match`("name", "0Fa1").build
    val query = matchCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[TermQuery], query.getClass)

    val termQuery = query.asInstanceOf[TermQuery]
    assertEquals("Query value is wrong", "0fa1", termQuery.getTerm.bytes().utf8ToString())
  }

  test("InetV4") {
    val schemaVal = schema().mapper("name", inetMapper()).build
    val matchCondition = `match`("name", "192.168.0.01").build
    val query = matchCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[TermQuery], query.getClass)

    val termQuery = query.asInstanceOf[TermQuery]
    assertEquals("Query value is wrong", "192.168.0.1", termQuery.getTerm.bytes().utf8ToString())
  }

  test("InetV6") {
    val schemaVal = schema().mapper("name", inetMapper()).build
    val matchCondition = `match`("name", "2001:DB8:2de::0e13").build
    val query = matchCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[TermQuery], query.getClass)

    val termQuery = query.asInstanceOf[TermQuery]
    assertEquals("Query value is wrong",
      "2001:db8:2de:0:0:0:0:e13",
      termQuery.getTerm.bytes().utf8ToString())
  }

  test("UnsupportedMapper") {
    intercept[IndexException] {
      val mapper = new MatchConditionTest.MockedMapper()
      val schemaVal = schema().mapper("field",
        new MatchConditionTest.MockedMapperBuilder(mapper)).build
      val matchCondition = `match`("field", "2001:DB8:2de::0e13").build
      matchCondition.doQuery(schemaVal)
    }.getMessage shouldBe s"fdñljps"
  }

  test("ToString") {
    val condition = `match`("name", "2001:DB8:2de::0e13").boost(0.5f).docValues(true).build
    assertEquals("Method #toString is wrong",
      "MatchCondition{boost=0.5, field=name, value=2001:DB8:2de::0e13, docValues=true}",
      condition.toString())
  }
}

object MatchConditionTest {

  class MockedMapper() extends SingleColumnMapper.SingleFieldMapper[UUID]("field",
                                                                          null,
                                                                          true,
                                                                          true,
                                                                          null,
                                                                          classOf[UUID],
                                                                          Collections.singletonList(classOf[UUID])) {

    override def indexedField(name: String, value: UUID): Optional[Field] = null

    override def sortedField(name: String, value: UUID): Optional[Field] = null

    override def doBase(name: String, value: Object): UUID = null

    override def sortField(
        name: String,
        reverse: Boolean): org.apache.lucene.search.SortField = null

  }

  class MockedMapperBuilder(mapper: MockedMapper) extends MapperBuilder[MockedMapper, MockedMapperBuilder] {
    override def build(field: String): MockedMapper = mapper
  }

}