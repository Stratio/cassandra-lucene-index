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
package com.stratio.cassandra.lucene.search

import com.google.common.collect.Sets
import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.schema.SchemaBuilders.{schema, stringMapper}
import com.stratio.cassandra.lucene.search.SearchBuilders._
import com.stratio.cassandra.lucene.search.condition.builder.ConditionBuilder
import com.stratio.cassandra.lucene.search.sort.builder.SortFieldBuilder
import org.apache.lucene.search.MatchAllDocsQuery
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * @author Andres de la Pena `adelapena@stratio.com`
 */
@RunWith(classOf[JUnitRunner])
class SearchTest extends BaseScalaTest {


    test("builder empty") {
        val query : Search = search().build
        assertEquals("Default refresh is not set", false, query.refresh)
    }

    test("Builder") {
        assertTrue("Refresh is not set", search().filter(SearchTest.MATCH)
                                                 .query(SearchTest.MATCH)
                                                 .sort(SearchTest.FIELD)
                                                 .refresh(true)
                                                 .build
                                                 .refresh)
    }

    test("uses relevance") {
        assertFalse("Use relevance is wrong", filter(SearchTest.MATCH).build.usesRelevance)
        assertTrue("Use relevance is wrong", query(SearchTest.MATCH).build.usesRelevance)
        assertFalse("Use relevance is wrong", sort(SearchTest.FIELD).build.usesRelevance)
        assertTrue("Use relevance is wrong", filter(SearchTest.MATCH).query(SearchTest.MATCH).sort(SearchTest.FIELD).build.usesRelevance)
        assertTrue("Use relevance is wrong", query(SearchTest.MATCH).sort(SearchTest.FIELD).build.usesRelevance)
        assertFalse("Use relevance is wrong", filter(SearchTest.MATCH).sort(SearchTest.FIELD).build.usesRelevance)
    }

    test("UsesSorting") {
        assertFalse("Use sorting is wrong", filter(SearchTest.MATCH).build.usesSorting)
        assertFalse("Use sorting is wrong", query(SearchTest.MATCH).build.usesSorting)
        assertTrue("Use sorting is wrong", sort(SearchTest.FIELD).build.usesSorting)
        assertTrue("Use sorting is wrong",
                   search().filter(`match`("field", "v"))
                           .query(`match`("field", "v"))
                           .sort(field("field"))
                           .build
                           .usesRelevance)
    }

    test("RequiresFullScan") {
        assertFalse("Requires full scan is wrong", filter(SearchTest.MATCH).build.requiresFullScan)
        assertTrue("Requires full scan is wrong", query(SearchTest.MATCH).build.requiresFullScan)
        assertFalse("Requires full scan is wrong", filter(SearchTest.MATCH).refresh(true).build.requiresFullScan)
        assertTrue("Requires full scan is wrong", sort(SearchTest.FIELD).build.requiresFullScan)
        assertTrue("Requires full scan is wrong", refresh(true).build.requiresFullScan)
        assertFalse("Requires full scan is wrong", refresh(false).build.requiresFullScan)
        assertFalse("Requires full scan is wrong", search().build.requiresFullScan)
        assertTrue("Requires full scan is wrong", search().query(SearchTest.MATCH).sort(SearchTest.FIELD).build.requiresFullScan)
        assertTrue("Requires full scan is wrong",
                   search().filter(SearchTest.MATCH).query(SearchTest.MATCH).sort(SearchTest.FIELD).build.requiresFullScan)
    }

    test("RequiresPostProcessing") {
        assertFalse("Requires post processing is wrong", filter(SearchTest.MATCH).build.requiresPostProcessing)
        assertTrue("Requires post processing is wrong", query(SearchTest.MATCH).build.requiresPostProcessing)
        assertFalse("Requires post processing is wrong", filter(SearchTest.MATCH).refresh(true).build.requiresPostProcessing)
        assertTrue("Requires post processing is wrong", sort(SearchTest.FIELD).build.requiresPostProcessing)
        assertFalse("Requires post processing is wrong", refresh(true).build.requiresPostProcessing)
        assertFalse("Requires post processing is wrong", refresh(false).build.requiresPostProcessing)
        assertFalse("Requires post processing is wrong", search().build.requiresPostProcessing)
        assertTrue("Requires post processing is wrong",
                   search().filter(SearchTest.MATCH).sort(SearchTest.FIELD).build.requiresPostProcessing)
        assertTrue("Requires post processing is wrong",
                   search().filter(SearchTest.MATCH).query(SearchTest.MATCH).sort(SearchTest.FIELD).build.requiresPostProcessing)
    }

    test("Sort") {
        val schemaVal = schema().mapper("f", stringMapper()).build
        assertNotNull("Sort fields is wrong", sort(SearchTest.FIELD).build.sortFields(schemaVal))
        assertTrue("Sort fields is wrong", filter(SearchTest.MATCH).build.sortFields(schemaVal).isEmpty)
    }

    test("Validate") {
        val schemaVal = schema().mapper("f", stringMapper()).build
        search().filter(SearchTest.MATCH)
          .query(SearchTest.MATCH)
          .sort(SearchTest.FIELD)
          .build
          .validate(schemaVal)
    }

    test("EmptyQuery") {
        val  query = search().build.query(schema().build, null)
        assertTrue("Pure negation is wrong", query.isInstanceOf[MatchAllDocsQuery])
    }

    test("PostProcessingFields") {
        assertEquals("postProcessingFields is wrong",
                     Sets.newHashSet("f2", "f3.f1", "f3.f2", "f3.f3"),
                     search().filter(`match`("f1", 1))
                       .query(`match`("f2", 1))
                       .query(bool.must(`match`("f3.f1", 1))
                         .must(`match`("f3.f2", 1))
                         .should(`match`("f3.f3", 1))
                         .not(`match`("f3.f4", 1)))
                       .build
                       .postProcessingFields)
    }

    test("ToString") {
        val searchVal = search().filter(Array[ConditionBuilder[_,_]](`match`("f1", "v1").docValues(true),`match`("f2", "v2")))
                                .query(Array[ConditionBuilder[_,_]](`match`("f3", "v3"), `match`("f4", "v4").boost(0.3f)))
                                .sort(field("f5").reverse(true))
                                .refresh(true)
                                .build
        assertEquals("Method #toString is wrong",
                     "Search{filter=[MatchCondition{boost=null, field=f1, value=v1, docValues=true}, " +
                     "MatchCondition{boost=null, field=f2, value=v2, docValues=false}], " +
                     "query=[MatchCondition{boost=null, field=f3, value=v3, docValues=false}, " +
                     "MatchCondition{boost=0.3, field=f4, value=v4, docValues=false}], " +
                     "sort=[SimpleSortField{field=f5, reverse=true}], refresh=true, paging=null}",
            searchVal.toString)
    }

}
object SearchTest {
    val MATCH : ConditionBuilder[_,_] = `match`("f", "v")
    val FIELD : SortFieldBuilder[_,_] = field("f")
}