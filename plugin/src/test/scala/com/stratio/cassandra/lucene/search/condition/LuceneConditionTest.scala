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

import com.google.common.collect.Sets
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.schema.SchemaBuilders.schema
import com.stratio.cassandra.lucene.search.SearchBuilders.lucene
import com.stratio.cassandra.lucene.search.condition.LuceneCondition.DEFAULT_FIELD
import com.stratio.cassandra.lucene.search.condition.builder.LuceneConditionBuilder
import org.apache.lucene.search.TermQuery
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * @author Andres de la Pena `adelapena@stratio.com`
 */
@RunWith(classOf[JUnitRunner])
class LuceneConditionTest extends AbstractConditionTest {

    test("Build") {
        val condition = new LuceneConditionBuilder("field:value").boost(0.7f).defaultField("field").build
        assertNotNull("Condition is not built", condition)
        assertEquals("Boost is not set", 0.7f, condition.boost, 0)
        assertEquals("Field is not set", "field", condition.defaultField)
        assertEquals("Query is not set", "field:value", condition.query)
    }

    test("BuildDefaults") {
        val condition = new LuceneConditionBuilder("field:value").build
        assertNotNull("Condition is not built", condition)
        assertNull("Boost is not set to default", condition.boost)
        assertEquals("Field is not set to default", DEFAULT_FIELD, condition.defaultField)
        assertEquals("Query is not set", "field:value", condition.query)
    }

    test("BuildWithoutQuery") {
        intercept[IndexException] {
            new LuceneConditionBuilder(null).build
        }.getMessage shouldBe s"fdñljps"
    }

    test("JsonSerialization") {
        val builder = new LuceneConditionBuilder("field:value").boost(0.7f).defaultField("field")
        testJsonSerialization(builder, "{type:\"lucene\",query:\"field:value\",boost:0.7,default_field:\"field\"}")
    }

    test("JsonSerializationDefaults") {
        val builder = new LuceneConditionBuilder("field:value")
        testJsonSerialization(builder, "{type:\"lucene\",query:\"field:value\"}")
    }

    test("Query") {
        val schemaVal = schema().defaultAnalyzer("english").build
        val condition = new LuceneCondition(0.7f, "field_1", "field_2:houses")

        val query = condition.doQuery(schemaVal)
        assertNotNull("Query is not built", query)
        assertEquals("Query type is wrong", classOf[TermQuery], query.getClass)

        val termQuery = query.asInstanceOf[TermQuery]
        assertEquals("Query term is wrong", "hous", termQuery.getTerm.bytes().utf8ToString())
    }

    test("QueryInvalid") {
        intercept[IndexException] {
            val schemaVal = schema().defaultAnalyzer("english").build
            val condition = new LuceneCondition(0.7f, "field_1", ":")
            condition.query(schemaVal)
        }.getMessage shouldBe s"fdñljps"
    }

    test("InvolvedFields") {
        assertEquals("Involved fields is wrong",
                     Sets.newHashSet("f0", "f1", "f2"),
                     lucene("f1:3 \t AND f2:").defaultField("f0").build.postProcessingFields())
        assertEquals("Involved fields is wrong with default field",
                     Sets.newHashSet(LuceneCondition.DEFAULT_FIELD),
                     lucene("f1 f2").build.postProcessingFields())
        assertEquals("Involved fields is wrong with complex expressions",
                     Sets.newHashSet(LuceneCondition.DEFAULT_FIELD, "date"),
                     lucene("\"jakarta apache\"^4 date:[20020101 TO 20030101]").build.postProcessingFields())
    }

    test("ToString") {
        val condition = new LuceneCondition(0.7f, "field_1", "field_2:houses")
        assertEquals("Method #toString is wrong",
                     "LuceneCondition{boost=0.7, query=field_2:houses, defaultField=field_1}",
                     condition.toString())
    }
}
