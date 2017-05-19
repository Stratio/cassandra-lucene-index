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

import com.stratio.cassandra.lucene.IndexOptions._
import com.stratio.cassandra.lucene.{BaseScalaTest, IndexException}
import com.stratio.cassandra.lucene.common.JsonSerializer
import com.stratio.cassandra.lucene.search.SearchBuilders._
import com.stratio.cassandra.lucene.search.sort.builder.SortFieldBuilder
import org.junit.Assert._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Class for testing [[SearchBuilder]].
 *
 * @author Andres de la Pena `<adelapena@stratio.com>`
 */
@RunWith(classOf[JUnitRunner])
class SearchBuilderTest extends BaseScalaTest {

    test("Build") {
        val filter = `match`("field1", "value2")
        val query = `match`("field2", "value2")
        val sort1 = field("field3")
        val sort2 = field("field4")
        val builder : SearchBuilder = search().filter(filter)
                                                   .query(query)
                                                   .sort(Array[SortFieldBuilder[_,_]](sort1, sort2))
        val json = builder.toJson
        assertEquals("JSON serialization is wrong", json, JsonSerializer.toString(builder))
    }

    test("Json") {
        val searchBuilder : SearchBuilder= search().filter(`match`("field1", "value1"))
                                                   .query(`match`("field2", "value2"))
                                                   .sort(field("field"))
        val json = searchBuilder.toJson
        assertEquals("JSON serialization is wrong", json, SearchBuilder.fromJson(json).toJson)
    }

    test("from invalid json") {
      intercept[IndexException] {
        SearchBuilder.fromJson("error")
      }.getMessage shouldBe s"dsmfldesw"
    }

    test("FromLegacyJsonWithOnlyQuery") {
        assertEquals("Legacy syntax parsing fails",
                     "Search{" +
                     "filter=[], " +
                     "query=[MatchCondition{boost=null, field=f, value=1, docValues=false}], sort=[], " +
                     "refresh=false, " +
                     "paging=null}",
                     SearchBuilder.fromJson("{query:{type: \"match\", field: \"f\", value:1}}").build.toString)
    }

    test("FromLegacyJsonWithOnlySort") {
        assertEquals("Legacy syntax parsing fails",
                     "Search{" +
                     "filter=[], " +
                     "query=[], " +
                     "sort=[SimpleSortField{field=f, reverse=false}], " +
                     "refresh=false, " +
                     "paging=null}",
                     SearchBuilder.fromJson("{sort:{fields:[{field:\"f\"}]}}").build.toString)
    }

    test("FromLegacyJsonWithAll") {
        assertEquals("Legacy syntax parsing fails",
                     "Search{" +
                     "filter=[MatchCondition{boost=null, field=f1, value=1, docValues=false}], " +
                     "query=[MatchCondition{boost=null, field=f2, value=2, docValues=false}], " +
                     "sort=[SimpleSortField{field=f, reverse=false}], " +
                     "refresh=true, " +
                     "paging=null}",
                     SearchBuilder.fromJson("{filter:{type: \"match\", field: \"f1\", value:1}, " +
                                            "query:{type: \"match\", field: \"f2\", value:2}, " +
                                            "sort:{fields:[{field:\"f\"}]}, " +
                                            "refresh:true}}").build.toString)
    }
}