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
package com.stratio.cassandra.lucene.search

import java.io.IOException

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.common.JsonSerializer
import com.stratio.cassandra.lucene.search.condition.builder.ConditionBuilder
import com.stratio.cassandra.lucene.search.sort.builder.SortFieldBuilder

/**
 * Class for building a new [[Search]] using the old syntax prior to 3.0.7.2.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @param filterBuilder the filtering condition to be set
 * @param queryBuilder the querying condition to be set
 * @param sortBuilder the sorting fields to be set
 * @param refresh {{{true]] if the [[Search]] to be built must refresh the Lucene's index searcher before
     * searching, {{{false]] otherwise
 * @param paging a paging state
 */
@Deprecated
class SearchBuilderLegacy @JsonCreator() (  @JsonProperty("filter") val filterBuilder : ConditionBuilder[_,_],
                                            @JsonProperty("query")  val queryBuilder : ConditionBuilder[_,_],
                                            @JsonProperty("sort")  val sortBuilder : SearchBuilderLegacy.SortBuilder,
                                            @JsonProperty("refresh") val refresh : Boolean,
                                            @JsonProperty("paging") val paging: String) {

    val builder: SearchBuilder = new SearchBuilder()


}

object SearchBuilderLegacy {
    /**
     * Returns the [[SearchBuilder]] represented by the specified JSON [[String]].
     *
     * @param json the JSON {{{String]] representing a [[SearchBuilder]]
     * @return the [[SearchBuilder]] represented by the specified JSON {{{String]]
     */
    def fromJson(json : String) : SearchBuilder = {
        try {
            JsonSerializer.fromString(json, classOf[SearchBuilder])
        } catch {
            case (e: IOException) =>throw new IndexException(e, "Unparseable JSON search: {}", json)
        }
    }

    class SortBuilder @JsonCreator()(@JsonProperty("fields") val fields : Array[SortFieldBuilder[_,_]])
}

