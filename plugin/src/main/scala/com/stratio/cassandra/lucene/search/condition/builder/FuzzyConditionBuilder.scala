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
package com.stratio.cassandra.lucene.search.condition.builder

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.stratio.cassandra.lucene.search.condition.{ContainsCondition, DateRangeCondition, FuzzyCondition}

/**
 * [[ConditionBuilder]] for building a new [[FuzzyCondition]].
 *
 * @author Andres de la Pena `adelapena@stratio.com`
  * @param field the name of the field to be matched.
  * @param value the value of the field to be matched.



 */
class FuzzyConditionBuilder @JsonCreator() (@JsonProperty("field") val field: String,
                                            @JsonProperty("value") val value: String) extends ConditionBuilder[FuzzyCondition, FuzzyConditionBuilder] {

    // maxEdits the Damerau-Levenshtein max distance
    var maxEdits: Int = FuzzyCondition.DEFAULT_MAX_EDITS

    // prefixLength the length of common (non-fuzzy) prefix
    var prefixLength: Int= FuzzyCondition.DEFAULT_PREFIX_LENGTH

    // maxExpansions the maximum number of terms to match
    var maxExpansions: Int= FuzzyCondition.DEFAULT_MAX_EXPANSIONS

    // transpositions If transpositions should be treated as a primitive edit operation.
    var transpositions: Boolean= FuzzyCondition.DEFAULT_TRANSPOSITIONS

    @JsonProperty("max_edits")
    def maxEdits(_maxEdits : Int): FuzzyConditionBuilder = {
        this.maxEdits=_maxEdits
        this
    }

    @JsonProperty("prefix_length")
    def prefixLength(_prefixLength : Int): FuzzyConditionBuilder = {
        this.prefixLength=_prefixLength
        this
    }

    @JsonProperty("max_expansions")
    def maxExpansions(_maxExpansions : Int): FuzzyConditionBuilder = {
        this.maxExpansions=_maxExpansions
        this
    }
    @JsonProperty("transpositions")
    def transpositions(_transpositions : Boolean): FuzzyConditionBuilder = {
        this.transpositions=_transpositions
        this
    }


    /** @inheritdoc*/
    override def build: FuzzyCondition =
        new FuzzyCondition(boost,
            field,
            value,
            maxEdits,
            prefixLength,
            maxExpansions,
            transpositions)
}