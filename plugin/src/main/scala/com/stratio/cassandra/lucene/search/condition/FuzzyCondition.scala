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

import com.google.common.base.MoreObjects
import com.stratio.cassandra.lucene.IndexException
import com.stratio.cassandra.lucene.schema.mapping.SingleColumnMapper
import org.apache.commons.lang3.StringUtils
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.Term
import org.apache.lucene.search.{FuzzyQuery, Query}
import org.apache.lucene.util.automaton.LevenshteinAutomata

/**
 * A [[Condition]] that implements the fuzzy search query. The similarity measurement is based on the
 * Damerau-Levenshtein (optimal string alignment) algorithm, though you can explicitly choose classic Levenshtein by
 * passing {{{false} to the {{{transpositions} parameter.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 *
 * @param boost The boost for this query clause. Documents matching this clause will (in addition to the normal
 * weightings) have their score multiplied by {{{boost}.
 * @param field the field name
 * @param value the field fuzzy value
 * @param maxEdits must be {@literal >=} 0 and `=} [[LevenshteinAutomata#MAXIMUM_SUPPORTED_DISTANCE}.
 * @param prefixLength length of common (non-fuzzy) prefix
 * @param maxExpansions The maximum number of terms to match. If this number is greater than {@link
 * org.apache.lucene.search.BooleanQuery#getMaxClauseCount} when the query is rewritten, then the maxClauseCount
 * will be used instead.
 * @param transpositions {{{true} if transpositions should be treated as a primitive edit operation. If this is
 * {{{false}, comparisons will implement the classic Levenshtein algorithm.
 */
class FuzzyCondition(   val boost : java.lang.Float,
                        val field: String,
                        val value: String,
                        var maxEdits: Int,
                        var prefixLength: Int,
                        var maxExpansions: Int,
                        var transpositions: Boolean) extends SingleColumnCondition(boost,field) {
    if (StringUtils.isBlank(value)) throw new IndexException("Field value required")

    if (maxEdits == null) {
        maxEdits=FuzzyCondition.DEFAULT_MAX_EDITS
    } else if (maxEdits < 0 || maxEdits > 2) {
        throw new IndexException("max_edits must be between 0 and 2")
    }

    if (prefixLength == null) {
        prefixLength = FuzzyCondition.DEFAULT_PREFIX_LENGTH
    } else if (prefixLength < 0) {
        throw new IndexException("prefix_length must be positive.")
    }

    if (maxExpansions == null) {
        maxExpansions = FuzzyCondition.DEFAULT_MAX_EXPANSIONS
    } else if (maxExpansions < 0) {
        throw new IndexException("max_expansions must be positive.")
    }

    if (transpositions == null) {
        transpositions = FuzzyCondition.DEFAULT_TRANSPOSITIONS
    }

    /** @inheritdoc */
    override def doQuery(mapper : SingleColumnMapper[_], analyzer : Analyzer) : Query = {
        if (mapper.base == classOf[String]) {
            val term : Term = new Term(field, value)
            new FuzzyQuery(term, maxEdits, prefixLength, maxExpansions, transpositions)
        } else {
            throw new IndexException(s"Fuzzy queries are not supported by mapper $mapper")
        }
    }

    /** @inheritdoc */
    override def toStringHelper: MoreObjects.ToStringHelper = {
        toStringHelper(this).add("value", value)
                                   .add("maxEdits", maxEdits)
                                   .add("prefixLength", prefixLength)
                                   .add("maxExpansions", maxExpansions)
                                   .add("transpositions", transpositions)
    }
}

object FuzzyCondition {

    /** The default Damerau-Levenshtein max distance. */
    val DEFAULT_MAX_EDITS : Int = LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE

    /** The default length of common (non-fuzzy) prefix. */
    val DEFAULT_PREFIX_LENGTH : Int = 0

    /** The default max expansions. */
    val DEFAULT_MAX_EXPANSIONS  : Int= 50

    /** If transpositions should be treated as a primitive edit operation by default. */
    val DEFAULT_TRANSPOSITIONS : Boolean = true
}