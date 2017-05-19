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
import com.stratio.cassandra.lucene.search.SearchBuilders
import com.stratio.cassandra.lucene.search.SearchBuilders._
import com.stratio.cassandra.lucene.search.condition.builder.{BooleanConditionBuilder, ConditionBuilder}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class BooleanConditionTest extends AbstractConditionTest {

  test("Build") {
    val all = SearchBuilders.all.asInstanceOf[ConditionBuilder[_, _]]
    val builder = new BooleanConditionBuilder().boost(0.7f).must(SearchBuilders.all)
      .must(SearchBuilders.all)
      .should(Array(all, all))
      .should(Array(all, all))
      .not(Array(all, all, all))
      .not(Array(all, all, all))
    val condition = builder.build
    assertNotNull("Condition is not built", condition)
    assertEquals("Boost is not set", 0.7f, condition.boost, 0)
    assertEquals("Must is not set", 2, condition.must.length)
    assertEquals("Should is not set", 4, condition.should.length)
    assertEquals("Not is not set", 6, condition.not.length)
  }

  test("BuildDefaults") {
    val builder = new BooleanConditionBuilder()
    val condition = builder.build
    assertNotNull("Condition is not built", condition)
    assertNull("Boost is not set to default", condition.boost)
    assertEquals("Must is not set", 0, condition.must.length)
    assertEquals("Should is not set", 0, condition.should.length)
    assertEquals("Not is not set", 0, condition.not.length)
  }

  test("JsonSerialization") {
    val builder = new BooleanConditionBuilder().boost(0.7f).must(SearchBuilders.all)
    testJsonSerialization(builder,
      "{type:\"boolean\",boost:0.7,must:[{type:\"all\"}],should:[],not:[]}")
  }

  test("JsonSerializationDefaults") {
    val builder = new BooleanConditionBuilder()
    testJsonSerialization(builder, "{type:\"boolean\",must:[],should:[],not:[]}")
  }

  test("Query") {
    val schemaVal = schema().mapper("name", stringMapper())
      .mapper("color", stringMapper())
      .mapper("country", stringMapper())
      .mapper("age", integerMapper())
      .defaultAnalyzer("default")
      .build
    val condition = bool.must(Array(`match`("name",
      "jonathan").asInstanceOf[ConditionBuilder[_, _]],
      range("age").lower(18).includeLower(true).asInstanceOf[ConditionBuilder[_, _]]))
      .should(Array(`match`("color", "green").asInstanceOf[ConditionBuilder[_, _]],
        `match`("color", "blue").asInstanceOf[ConditionBuilder[_, _]]))
      .not(`match`("country", "england").asInstanceOf[ConditionBuilder[_, _]])
      .boost(0.4f)
      .build
    val query = condition.doQuery(schemaVal)
    assertEquals("Query count clauses is wrong", 5, query.clauses().size())
  }

  test("QueryEmpty") {
    val schemaVal = schema().build
    val condition = bool.boost(0.4f).build
    val query = condition.doQuery(schemaVal)
    assertEquals("Query count clauses is wrong", 0, query.clauses().size())
  }

  test("QueryPureNot") {
    val schemaVal = schema().mapper("name", stringMapper()).build
    val condition = bool.not(`match`("name",
      "jonathan").asInstanceOf[ConditionBuilder[_, _]]).boost(0.4f).build
    val query = condition.doQuery(schemaVal)
    assertEquals("Query count clauses is wrong", 2, query.clauses().size())
  }

  test("ToString") {
    val condition = bool.must(Array(`match`("name",
      "jonathan").asInstanceOf[ConditionBuilder[_, _]],
      `match`("age", 18).asInstanceOf[ConditionBuilder[_, _]]))
      .should(`match`("color", "green").asInstanceOf[ConditionBuilder[_, _]])
      .not(`match`("country", "england").boost(0.7f).asInstanceOf[ConditionBuilder[_, _]])
      .boost(0.5f)
      .build
    assertEquals("Method #toString is wrong",
      "BooleanCondition{boost=0.5, " +
        "must=[MatchCondition{boost=null, field=name, value=jonathan, docValues=false}, " +
        "MatchCondition{boost=null, field=age, value=18, docValues=false}], " +
        "should=[MatchCondition{boost=null, field=color, value=green, docValues=false}], " +
        "not=[MatchCondition{boost=0.7, field=country, value=england, docValues=false}]}",
      condition.toString)
  }
}
