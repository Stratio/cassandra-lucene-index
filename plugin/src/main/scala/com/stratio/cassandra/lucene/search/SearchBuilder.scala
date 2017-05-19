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
import com.stratio.cassandra.lucene.common.{Builder, JsonSerializer}
import com.stratio.cassandra.lucene.search.condition.Condition
import com.stratio.cassandra.lucene.search.condition.builder.ConditionBuilder
import com.stratio.cassandra.lucene.search.sort.SortField
import com.stratio.cassandra.lucene.search.sort.builder.SortFieldBuilder
import com.stratio.cassandra.lucene.util.ByteBufferUtils
import com.stratio.cassandra.lucene.{IndexException, IndexPagingState}

/**
 * Class for building a new [[Search]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 */
class SearchBuilder() extends Builder[Search] {

    /** The filtering conditions not participating in scoring. */
    private[this] var filter : Array[ConditionBuilder[_,_]]  = Array[ConditionBuilder[_,_]]()

    /** The querying conditions participating in scoring. */
    private[this] var query: Array[ConditionBuilder[_,_]] = Array[ConditionBuilder[_,_]]()

    /** The [[SortFieldBuilder]]s for the query. */
    private[this] var sort: Array[SortFieldBuilder[_,_]] = Array[SortFieldBuilder[_,_]]()

    /** If this search must force the refresh the index before reading it. */
    private[this] var refresh: Boolean= false

    private[this] var paging : String =""


    /**
     * Returns this builder with the specified filtering conditions not participating in scoring.
     *
     * @param builders the conditions to be added
     * @return this builder with the specified conditions
     */
    @JsonProperty("filter")
    def filter(builders: Array[ConditionBuilder[_,_]]): SearchBuilder = {
        filter = filter ++ builders
        this
    }

    def filter(builder: ConditionBuilder[_,_]): SearchBuilder = {
      filter = filter :+ builder
      this
    }

    /**
     * Returns this builder with the specified querying conditions participating in scoring.
     *
     * @param builders the conditions to be added
     * @return this builder with the specified conditions
     */
    @JsonProperty("query")
    def query(builders: Array[ConditionBuilder[_,_]]): SearchBuilder = {
        query = query ++ builders
        this
    }

    def query(builder: ConditionBuilder[_,_]): SearchBuilder = {
      query = query :+ builder
      this
    }
    /**
     * Adds the specified sorting fields.
     *
     * @param builders the sorting fields to be added
     * @return this builder with the specified sorting fields
     */
    @JsonProperty("sort")
    def sort(builders: Array[SortFieldBuilder[_,_]]) : SearchBuilder = {
        sort = sort ++ builders
        this
    }

    def sort(builder: SortFieldBuilder[_,_]) : SearchBuilder = {
      sort = sort :+ builder
      this
    }
    /**
     * Sets if the [[Search]] to be built must refresh the index before reading it. Refresh is a costly operation so
     * you should use it only when it is strictly required.
     *
     * @param refresh {{{true]] if the [[Search]] to be built must refresh the Lucene's index searcher before
     *                 searching, {{{false]] otherwise
     *                 @return this builder with the specified refresh
      *               */
    @JsonProperty("refresh")
    def refresh(refresh: Boolean) :SearchBuilder = {
        this.refresh = refresh
        this
    }

    /**
     * Sets the specified starting partition key.
     *
     * @param pagingState a paging state
     * @return this builder with the specified partition key
     */
    @JsonProperty("paging")
    def paging( pagingState: IndexPagingState) : SearchBuilder = {
        this.paging = ByteBufferUtils.toHex(pagingState.toByteBuffer)
        this
    }

    /**
     * Returns the [[Search]] represented by this builder.
     *
     * @return the search represented by this builder
     */
    override def build: Search = {
      val filters: Array[Condition] = filter.map(_.build.asInstanceOf[Condition])
      val queries: Array[Condition] = query.map(_.build.asInstanceOf[Condition])
      val sorts: Array[SortField] = sort.map(_.build.asInstanceOf[SortField])

      new Search(filters,
        queries,
        sorts,
        if (paging == null) null
        else IndexPagingState.fromByteBuffer(ByteBufferUtils.byteBuffer(paging)),
        refresh)
    }
    /**
     * Returns the JSON representation of this object.
     *
     * @return a JSON representation of this object
     */
    def toJson:String = {
        build
        try {
            JsonSerializer.toString(this)
        } catch {
            case(e:IOException ) => throw new IndexException(e, s"Unformateable JSON search: ${e.getMessage}")
        }
    }
}
object SearchBuilder {
    /**
      * Returns the [[SearchBuilder]] represented by the specified JSON [[String]].
     *
     * @param json the JSON [[String]] representing a [[SearchBuilder]]
     * @return the [[SearchBuilder]] represented by the specified JSON {{{String]]
      *        */
    @JsonCreator
    def fromJson(json: String) : SearchBuilder = {
        try {
            JsonSerializer.fromString(json, classOf[SearchBuilder])
        } catch {
            case (e: IOException) => SearchBuilderLegacy.fromJson(json)
        }
    }
}