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

import java.util

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.IndexPagingState
import com.stratio.cassandra.lucene.schema.Schema
import com.stratio.cassandra.lucene.search.condition.Condition
import com.stratio.cassandra.lucene.search.sort.SortField
import com.stratio.cassandra.lucene.util.Logging
import org.apache.lucene.search
import org.apache.lucene.search.BooleanClause.Occur.{FILTER, MUST}
import org.apache.lucene.search._

/**
 * Class representing an Lucene index search. It can be translated to a Lucene [[Query]] using a [[Schema]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @param filter the filtering [[Condition]]s not involved in scoring
 * @param query the querying [[Condition]]s participating in scoring
 * @param sort the sort fields for the query
 * @param paging the paging state
 * @param refresh if this search must refresh the index before reading it

  */
class Search(var filter : Array[Condition],
    var query : Array[Condition],
    var sort : Array[SortField],
    var paging : IndexPagingState,
    var refresh: Boolean) extends Logging {


  this.filter = if (filter == null) Array[Condition]() else filter
  this.query = if (query == null) Array[Condition]() else query
  this.sort = if (sort == null) Array[SortField]() else sort
  this.paging = paging
  this.refresh = if (refresh == null) Search.DEFAULT_FORCE_REFRESH else refresh

    /**
     * Returns if this search requires post reconciliation agreement processing to preserve the order of its results.
     *
     * @return {{{true]] if it requires post processing, {{{false]] otherwise
     */
    def requiresPostProcessing : Boolean =
        usesRelevance || usesSorting

    /**
     * Returns if this search requires full ranges scan.
     *
     * @return {{{true}}} if this search requires full ranges scan, {{{{null}}} otherwise
     */
    def requiresFullScan : Boolean = usesRelevance || usesSorting || (refresh && isEmpty)


    /**
     * Returns if this search uses Lucene relevance formula.
     *
     * @return {{{true]] if this search uses Lucene relevance formula, {{{false]] otherwise
     */
    def usesRelevance : Boolean =
        query.nonEmpty

    /**
     * Returns if this search uses field sorting.
     *
     * @return {{{true]] if this search uses field sorting, {{{false]] otherwise
     */
    def usesSorting :  Boolean  =
        sort.nonEmpty


    /**
     * Returns if this search doesn't specify any filter, query or sort.
     *
     * @return {{{true]] if this search doesn't specify any filter, query or sort, {{{false]] otherwise
     */
    def isEmpty: Boolean = filter.isEmpty && query.isEmpty && sort.isEmpty


    /**
     * Returns the Lucene [[Query]] represented by this search, with the additional optional data range filter.
     *
     * @param schema the indexing schema
     * @param range the additional data range filter, maybe {{{null]]
     * @return a Lucene [[Query]]
     */
    def query(schema : Schema , range : Query ): Query = {

        val builder : BooleanQuery.Builder = {
          new BooleanQuery.Builder()
        }
        if (range != null) {
            builder.add(range, FILTER)
        }

        filter.foreach(condition => builder.add(condition.query(schema), FILTER))
        query.foreach(condition => builder.add(condition.query(schema), MUST))

        val booleanQuery : BooleanQuery = builder.build()
        if (booleanQuery.clauses().isEmpty) new MatchAllDocsQuery() else booleanQuery
    }

    def postProcessingQuery(schema:Schema) : Query = {
        if (query.isEmpty) {
            new MatchAllDocsQuery()
        } else {
            val builder : BooleanQuery.Builder = new BooleanQuery.Builder()
            query.foreach(condition => builder.add(condition.query(schema), MUST))
            builder.build()
        }
    }

    /**
     * Returns the Lucene [[search.SortField]]s represented by this using the specified schema.
     *
     * @param schema the indexing schema to be used
     * @return the Lucene sort fields represented by this using {{{schema]]
      *        */
    def sortFields(schema: Schema) : Array[search.SortField] = {
        sort.toStream.map(_.sortField(schema)).toArray
    }


    /**
     * Returns the names of the involved fields when post processing.
     *
     * @return the names of the involved fields
     */
    def postProcessingFields : util.Set[String] = {
        val fields : util.Set[String]  = new util.HashSet
        query.foreach(condition => fields.addAll(condition.postProcessingFields))
        sort.foreach(sortField => fields.addAll(sortField.postProcessingFields))
        fields
    }

    /**
     * Validates this [[Search]] against the specified [[Schema]].
     *
     * @param schema a [[Schema]]
     * @return this
     */
    def validate(schema: Schema) : Search = {
        filter.foreach(condition => condition.query(schema))
        query.foreach(condition => condition.query(schema))
        sort.foreach(field => field.sortField(schema))
        this
    }

    /** @inheritdoc */
    override def toString: String = MoreObjects.toStringHelper(this)
                          .add("filter", filter)
                          .add("query", query)
                          .add("sort", sort)
                          .add("refresh", refresh)
                          .add("paging", paging)
                          .toString
}
object Search {
    def DEFAULT_FORCE_REFRESH :Boolean = false
}