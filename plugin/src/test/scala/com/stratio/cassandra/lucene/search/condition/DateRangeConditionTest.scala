/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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

import com.stratio.cassandra.lucene.common.GeoOperation
import com.stratio.cassandra.lucene.schema.SchemaBuilders._
import com.stratio.cassandra.lucene.search.SearchBuilders.dateRange
import com.stratio.cassandra.lucene.search.condition.DateRangeCondition._
import com.stratio.cassandra.lucene.search.condition.builder.DateRangeConditionBuilder
import org.apache.lucene.spatial.prefix.IntersectsPrefixTreeQuery
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class DateRangeConditionTest extends AbstractConditionTest {

  test("BuildString") {
    val builder = new DateRangeConditionBuilder("field").boost(0.4f).from("2015/01/05")
      .to("2015/01/08")
      .operation(GeoOperation.INTERSECTS)
    val condition = builder.build
    assertNotNull("Condition is not built", condition)
    assertEquals("Boost is not set", 0.4f, condition.boost, 0)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("From is not set", "2015/01/05", condition.from)
    assertEquals("To is not set", "2015/01/08", condition.to)
    assertEquals("Operation is not set", "intersects", condition.operation)
  }

  test("BuildNumber") {
    val builder = new DateRangeConditionBuilder("field").boost(0.4f)
      .from(1)
      .to(2)
      .operation(GeoOperation.IS_WITHIN)
    val condition = builder.build
    assertNotNull("Condition is not built", condition)
    assertEquals("Boost is not set", 0.4f, condition.boost, 0)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("From is not set", 1, condition.from)
    assertEquals("To is not set", 2, condition.to)
    assertEquals("Operation is not set", "is_within", condition.operation)
  }

  test("BuildDefaults") {
    val builder = new DateRangeConditionBuilder("field")
    val condition = builder.build
    assertNotNull("Condition is not built", condition)
    assertNull("Boost is not set to default", condition.boost)
    assertEquals("Field is not set", "field", condition.field)
    assertEquals("From is not set to default", DEFAULT_FROM, condition.from)
    assertEquals("To is not set to default", DEFAULT_TO, condition.to)
    assertEquals("Operation is not set to default", DEFAULT_OPERATION, condition.operation)
  }

  test("JsonSerializationString") {
    val builder = new DateRangeConditionBuilder("field").boost(0.4f)
      .from("from")
      .to("to")
      .operation(GeoOperation.INTERSECTS)
    testJsonSerialization(builder,
      "{type:\"date_range\",field:\"field\",boost:0.4," +
        "from:\"from\",to:\"to\",operation:\"intersects\"}")
  }

  test("JsonSerializationNumber") {
    val builder = new DateRangeConditionBuilder("field").boost(0.4f)
      .from("2015/01/05")
      .to("2015/01/08")
      .operation(GeoOperation.CONTAINS)
    testJsonSerialization(builder,
      "{type:\"date_range\",field:\"field\",boost:0.4," +
        "from:\"2015/01/05\",to:\"2015/01/08\",operation:\"contains\"}")
  }

  test("JsonSerializationDefaults") {
    val builder = new DateRangeConditionBuilder("field")
    testJsonSerialization(builder, "{type:\"date_range\",field:\"field\"}")
  }

  test("Query") {
    val schemaVal = schema().mapper("name",
      dateRangeMapper("from", "to").pattern("yyyyMMdd Z")).build
    val condition = dateRange("name").from("20160305 PST").to("20160405 PST").build
    val query = condition.query(schemaVal)
    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[IntersectsPrefixTreeQuery], query.getClass)
    assertEquals("Query is wrong",
      "IntersectsPrefixTreeQuery(fieldName=name,queryShape=" +
        "[2016-03-05T08 TO 2016-04-05T08:00:00.000],detailLevel=9,prefixGridScanLevel=7)",
      query.toString())
  }

  test("QueryOpenStart") {
    val schemaVal = schema().mapper("name",
      dateRangeMapper("from", "to").pattern("yyyyMMdd Z")).build
    val condition = dateRange("name").to("20160305 PST").build
    val query = condition.query(schemaVal)
    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[IntersectsPrefixTreeQuery], query.getClass)
    assertEquals("Query is wrong",
      "IntersectsPrefixTreeQuery(fieldName=name,queryShape=" +
        "[-292269054-12-02T16:47:04.192 TO 2016-03-05T08:00:00.000],detailLevel=9,prefixGridScanLevel=7)",
      query.toString())
  }

  test("QueryOpenStop") {
    val schemaVal = schema().mapper("name",
      dateRangeMapper("from", "to").pattern("yyyyMMdd Z")).build
    val condition = dateRange("name").from("20160305 PST").build
    val query = condition.query(schemaVal)
    assertNotNull("Query is not built", query)
    assertEquals("Query type is wrong", classOf[IntersectsPrefixTreeQuery], query.getClass)
    assertEquals("Query is wrong",
      "IntersectsPrefixTreeQuery(fieldName=name,queryShape=" +
        "[2016-03-05T08 TO 292278994-08-17T07:12:55.807],detailLevel=9,prefixGridScanLevel=7)",
      query.toString())
  }

  test("QueryWithoutValidMapper") {
    val schemaVal = schema().mapper("name", uuidMapper()).build
    val condition = dateRange("name").from(1L).to(2L).build
    condition.query(schemaVal)
  }

  test("ToString") {
    val condition = dateRange("name").from(1).to(2).operation(GeoOperation.CONTAINS).boost(0.3f).build
    assertEquals("Method #toString is wrong",
      "DateRangeCondition{boost=0.3, field=name, from=1, to=2, operation=contains}",
      condition.toString)
  }
}
