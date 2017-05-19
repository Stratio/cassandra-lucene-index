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
import com.stratio.cassandra.lucene.search.SearchBuilders.range
import com.stratio.cassandra.lucene.search.condition.builder.{PrefixConditionBuilder, RangeConditionBuilder}
import org.apache.lucene.search.{NumericRangeQuery, TermRangeQuery}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class RangeConditionTest extends AbstractConditionTest {

  test("BuildString") {
    val condition = new RangeConditionBuilder("field").boost(0.4f)
      .lower("lower")
      .upper("upper")
      .includeLower(false)
      .includeUpper(true).build
    assertNotNull("Condition is not built", condition)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Boost is not set", 0.4f, condition.boost, 0)
    assertEquals("Lower is not set", "lower", condition.lower)
    assertEquals("Upper is not set", "upper", condition.upper)
    assertEquals("Include lower is not set", false, condition.includeLower)
    assertEquals("Include upper is not set", true, condition.includeUpper)
    assertEquals("Use doc values is not set",
      RangeCondition.DEFAULT_DOC_VALUES,
      condition.docValues)
  }

  test("BuildNumber") {
    val condition = new RangeConditionBuilder("field").boost(0.4f).lower(1).upper(2).includeLower(
      false).includeUpper(true).docValues(true).build
    assertNotNull("Condition is not built", condition)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("Boost is not set", 0.4f, condition.boost, 0)
    assertEquals("Lower is not set", 1, condition.lower)
    assertEquals("Upper is not set", 2, condition.upper)
    assertEquals("Include lower is not set", false, condition.includeLower)
    assertEquals("Include upper is not set", true, condition.includeUpper)
    assertEquals("Use doc values is not set", true, condition.docValues)
  }

  test("BuildDefaults") {
    val condition = new RangeConditionBuilder("field").build
    assertNotNull("Condition is not built", condition)
    assertEquals("Field is not set", "field", condition.field)
    assertNull("Boost is not set to default", condition.boost)
    assertNull("Lower is not set to default", condition.lower)
    assertNull("Upper is not set to default", condition.upper)
    assertEquals("Include Lower is not set to default",
      RangeCondition.DEFAULT_INCLUDE_LOWER,
      condition.includeLower)
    assertEquals("Include upper is not set to default",
      RangeCondition.DEFAULT_INCLUDE_UPPER,
      condition.includeUpper)
  }

  test("JsonSerializationString") {
    val builder = new RangeConditionBuilder("field").boost(0.4f)
      .lower("lower")
      .upper("upper")
      .includeLower(false)
      .includeUpper(true)
      .docValues(true)
    testJsonSerialization(builder,
      "{type:\"range\",field:\"field\",boost:0.4,lower:\"lower\",upper:\"upper\"," +
        "include_lower:false,include_upper:true,doc_values:true}")
  }

  test("JsonSerializationNumber") {
    val builder = new RangeConditionBuilder("field").boost(0.4f)
      .lower(1)
      .upper(2)
      .includeLower(false)
      .includeUpper(true)
      .docValues(true)
    testJsonSerialization(builder,
      "{type:\"range\",field:\"field\",boost:0.4,lower:1,upper:2," +
        "include_lower:false,include_upper:true,doc_values:true}")
  }

  test("JsonSerializationDefaults") {
    val builder = new PrefixConditionBuilder("field", "value")
    testJsonSerialization(builder, "{type:\"prefix\",field:\"field\",value:\"value\"}")
  }

  test("StringClose") {
    val schemaVal = schema().mapper("name", stringMapper()).build

    val rangeCondition = new RangeCondition(0.5f, "name", "alpha", "beta", true, true, false)
    val query = rangeCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[TermRangeQuery], query.getClass)

    val termRangeQuery = query.asInstanceOf[TermRangeQuery]
    assertEquals("Query field is wrong", "name", termRangeQuery.getField)
    assertEquals("Query lower is wrong", "alpha", termRangeQuery.getLowerTerm.utf8ToString())
    assertEquals("Query upper is wrong", "beta", termRangeQuery.getUpperTerm.utf8ToString())
    assertEquals("Query include lower is wrong", true, termRangeQuery.includesLower())
    assertEquals("Query include upper is wrong", true, termRangeQuery.includesUpper())
  }

  test("StringOpen") {
    val schemaVal = schema().mapper("name", stringMapper()).build

    val rangeCondition = new RangeCondition(0.5f, "name", "alpha", null, true, false, false)
    val query = rangeCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[TermRangeQuery], query.getClass)

    val termRangeQuery = query.asInstanceOf[TermRangeQuery]
    assertEquals("Query field is wrong", "name", termRangeQuery.getField)
    assertEquals("Query lower is wrong", "alpha", termRangeQuery.getLowerTerm.utf8ToString())
    assertEquals("Query upper is wrong", null, termRangeQuery.getUpperTerm)
    assertEquals("Query include lower is wrong", true, termRangeQuery.includesLower())
    assertEquals("Query include upper is wrong", false, termRangeQuery.includesUpper())
  }

  test("IntegerClose") {
    val schemaVal = schema().mapper("name", integerMapper().boost(1f)).build

    val rangeCondition = new RangeCondition(0.5f, "name", 42, 43, false, false, false)
    val query = rangeCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[NumericRangeQuery], query.getClass)

    val numericRangeQuery = query.asInstanceOf[NumericRangeQuery]
    assertEquals("Query field is wrong", "name", numericRangeQuery.getField)
    assertEquals("Query lower is wrong", 42, numericRangeQuery.getMin)
    assertEquals("Query upper is wrong", 43, numericRangeQuery.getMax)
    assertEquals("Query include lower is wrong", false, numericRangeQuery.includesMin())
    assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax())
  }

  test("IntegerOpen") {
    val schemaVal = schema().mapper("name", integerMapper().boost(1f)).build

    val rangeCondition = new RangeCondition(0.5f, "name", 42, null, true, false, false)
    val query = rangeCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[NumericRangeQuery], query.getClass)

    val numericRangeQuery = query.asInstanceOf[NumericRangeQuery]
    assertEquals("Query field is wrong", "name", numericRangeQuery.getField)
    assertEquals("Query lower is wrong", 42, numericRangeQuery.getMin)
    assertEquals("Query upper is wrong", null, numericRangeQuery.getMax)
    assertEquals("Query include lower is wrong", true, numericRangeQuery.includesMin())
    assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax())
  }

  test("LongClose") {
    val schemaVal = schema().mapper("name", longMapper().boost(1f)).build

    val rangeCondition = new RangeCondition(0.5f, "name", 42L, 43, false, false, false)
    val query = rangeCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[NumericRangeQuery], query.getClass)

    val numericRangeQuery = query.asInstanceOf[NumericRangeQuery]
    assertEquals("Query field is wrong", "name", numericRangeQuery.getField)
    assertEquals("Query lower is wrong", 42L, numericRangeQuery.getMin)
    assertEquals("Query upper is wrong", 43L, numericRangeQuery.getMax)
    assertEquals("Query include lower is wrong", false, numericRangeQuery.includesMin())
    assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax())
  }

  test("LongOpen") {
    val schemaVal = schema().mapper("name", longMapper().boost(1f)).build

    val rangeCondition = new RangeCondition(0.5f, "name", 42f, null, true, false, false)
    val query = rangeCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[NumericRangeQuery], query.getClass)

    val numericRangeQuery = query.asInstanceOf[NumericRangeQuery]
    assertEquals("Query field is wrong", "name", numericRangeQuery.getField)
    assertEquals("Query lower is wrong", 42L, numericRangeQuery.getMin)
    assertEquals("Query upper is wrong", null, numericRangeQuery.getMax)
    assertEquals("Query include lower is wrong", true, numericRangeQuery.includesMin())
    assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax())
  }

  test("FloatClose") {
    val schemaVal = schema().mapper("name", floatMapper().boost(1f)).build

    val rangeCondition = new RangeCondition(0.5f, "name", 42.42D, 43.42F, false, false, false)
    val query = rangeCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[NumericRangeQuery], query.getClass)

    val numericRangeQuery = query.asInstanceOf[NumericRangeQuery]
    assertEquals("Query field is wrong", "name", numericRangeQuery.getField)
    assertEquals("Query lower is wrong", 42.42F, numericRangeQuery.getMin)
    assertEquals("Query upper is wrong", 43.42f, numericRangeQuery.getMax)
    assertEquals("Query include lower is wrong", false, numericRangeQuery.includesMin())
    assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax())
  }

  test("FloatOpen") {
    val schemaVal = schema().mapper("name", floatMapper().boost(1f)).build

    val rangeCondition = new RangeCondition(0.5f, "name", 42.42f, null, true, false, false)
    val query = rangeCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[NumericRangeQuery], query.getClass)

    val numericRangeQuery = query.asInstanceOf[NumericRangeQuery]
    assertEquals("Query field is wrong", "name", numericRangeQuery.getField)
    assertEquals("Query lower is wrong", 42.42f, numericRangeQuery.getMin)
    assertEquals("Query upper is wrong", null, numericRangeQuery.getMax)
    assertEquals("Query include lower is wrong", true, numericRangeQuery.includesMin())
    assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax())
  }

  test("DoubleClose") {
    val schemaVal = schema().mapper("name", doubleMapper().boost(1f)).build

    val rangeCondition = new RangeCondition(0.5f, "name", 42.42D, 43.42D, false, false, false)
    val query = rangeCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[NumericRangeQuery], query.getClass)

    val numericRangeQuery = query.asInstanceOf[NumericRangeQuery]
    assertEquals("Query field is wrong", "name", numericRangeQuery.getField)
    assertEquals("Query lower is wrong", 42.42D, numericRangeQuery.getMin)
    assertEquals("Query upper is wrong", 43.42D, numericRangeQuery.getMax)
    assertEquals("Query include lower is wrong", false, numericRangeQuery.includesMin())
    assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax())
  }

  test("DoubleOpen") {
    val schemaVal = schema().mapper("name", doubleMapper().boost(1f)).build

    val rangeCondition = new RangeCondition(0.5f, "name", 42.42D, null, true, false, false)
    val query = rangeCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[NumericRangeQuery], query.getClass)

    val numericRangeQuery = query.asInstanceOf[NumericRangeQuery]
    assertEquals("Query field is wrong", "name", numericRangeQuery.getField)
    assertEquals("Query lower is wrong", 42.42D, numericRangeQuery.getMin)
    assertEquals("Query upper is wrong", null, numericRangeQuery.getMax)
    assertEquals("Query include lower is wrong", true, numericRangeQuery.includesMin())
    assertEquals("Query include upper is wrong", false, numericRangeQuery.includesMax())
  }

  test("InetV4") {
    val schemaVal = schema().mapper("name", inetMapper()).build

    val rangeCondition = new RangeCondition(0.5f,
      "name",
      "192.168.0.01",
      "192.168.0.045",
      true,
      true,
      false)
    val query = rangeCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[TermRangeQuery], query.getClass)

    val termRangeQuery = query.asInstanceOf[TermRangeQuery]
    assertEquals("Query field is wrong", "name", termRangeQuery.getField)
    assertEquals("Query lower is wrong", "192.168.0.1", termRangeQuery.getLowerTerm.utf8ToString())
    assertEquals("Query upper is wrong", "192.168.0.45", termRangeQuery.getUpperTerm.utf8ToString())
    assertEquals("Query include lower is wrong", true, termRangeQuery.includesLower())
    assertEquals("Query include upper is wrong", true, termRangeQuery.includesUpper())
  }

  test("InetV6") {
    val schemaVal = schema().mapper("name", inetMapper()).build

    val rangeCondition = range("name").boost(0.5f)
      .lower("2001:DB8:2de::e13")
      .upper("2001:DB8:02de::e23")
      .includeLower(true)
      .includeUpper(true)
      .build
    val query = rangeCondition.doQuery(schemaVal)

    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[TermRangeQuery], query.getClass)

    val termRangeQuery = query.asInstanceOf[TermRangeQuery]
    assertEquals("Query field is wrong", "name", termRangeQuery.getField)
    assertEquals("Query lower is wrong",
      "2001:db8:2de:0:0:0:0:e13",
      termRangeQuery.getLowerTerm.utf8ToString())
    assertEquals("Query upper is wrong",
      "2001:db8:2de:0:0:0:0:e23",
      termRangeQuery.getUpperTerm.utf8ToString())
    assertEquals("Query include lower is wrong", true, termRangeQuery.includesLower())
    assertEquals("Query include upper is wrong", true, termRangeQuery.includesUpper())
  }

  test("ToString") {
    val condition = range("name").boost(0.5f)
      .lower("2001:DB8:2de::e13")
      .upper("2001:DB8:02de::e23")
      .includeLower(true)
      .includeUpper(true)
      .docValues(true)
      .build
    assertEquals("Method #toString is wrong",
      "RangeCondition{boost=0.5, field=name, lower=2001:DB8:2de::e13, upper=2001:DB8:02de::e23, " +
        "includeLower=true, includeUpper=true, docValues=true}",
      condition.toString())
  }
}
