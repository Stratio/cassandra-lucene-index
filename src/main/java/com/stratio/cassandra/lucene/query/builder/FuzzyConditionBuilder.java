/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.query.builder;

import com.stratio.cassandra.lucene.query.FuzzyCondition;

/**
 * {@link ConditionBuilder} for building a new {@link FuzzyCondition}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class FuzzyConditionBuilder extends ConditionBuilder<FuzzyCondition, FuzzyConditionBuilder> {

    /** The name of the field to be matched. */
    private final String field;

    /** The fuzzy expression to be matched. */
    private final String value;

    /** The Damerau-Levenshtein max distance. */
    private Integer maxEdits;

    /** The length of common (non-fuzzy) prefix. */
    private Integer prefixLength;

    /** The maximum number of terms to match. */
    private Integer maxExpansions;

    /** If transpositions should be treated as a primitive edit operation. */
    private Boolean transpositions;

    /**
     * Returns a new {@link FuzzyConditionBuilder}
     *
     * @param field the name of the field to be matched.
     * @param value the value of the field to be matched.
     */
    protected FuzzyConditionBuilder(String field, String value) {
        this.field = field;
        this.value = value;
    }

    /**
     * Returns this builder with the specified Damerau-Levenshtein max distance.
     *
     * @param maxEdits The Damerau-Levenshtein max distance.
     * @return This builder with the specified Damerau-Levenshtein max distance.
     */
    public FuzzyConditionBuilder maxEdits(Integer maxEdits) {
        this.maxEdits = maxEdits;
        return this;
    }

    /**
     * Returns this builder with the length of common (non-fuzzy) prefix.
     *
     * @param prefixLength The length of common (non-fuzzy) prefix.
     * @return This builder with the length of common (non-fuzzy) prefix.
     */
    public FuzzyConditionBuilder prefixLength(Integer prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    /**
     * Returns this builder with the specified maximum number of terms to match.
     *
     * @param maxExpansions The maximum number of terms to match.
     * @return This builder with the specified maximum number of terms to match.
     */
    public FuzzyConditionBuilder maxExpansions(Integer maxExpansions) {
        this.maxExpansions = maxExpansions;
        return this;
    }

    /**
     * Returns this builder with the specified  if transpositions should be treated as a primitive edit operation.
     *
     * @param transpositions If transpositions should be treated as a primitive edit operation.
     * @return This builder with the specified  if transpositions should be treated as a primitive edit operation.
     */
    public FuzzyConditionBuilder transpositions(Boolean transpositions) {
        this.transpositions = transpositions;
        return this;
    }

    /**
     * Returns the {@link FuzzyCondition} represented by this builder.
     *
     * @return The {@link FuzzyCondition} represented by this builder.
     */
    @Override
    public FuzzyCondition build() {
        return new FuzzyCondition(boost, field, value, maxEdits, prefixLength, maxExpansions, transpositions);
    }
}
