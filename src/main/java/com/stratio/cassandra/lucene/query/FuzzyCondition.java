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
package com.stratio.cassandra.lucene.query;

import com.google.common.base.Objects;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.schema.mapping.ColumnMapperSingle;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link Condition} that implements the fuzzy search query. The similarity measurement is based on the
 * Damerau-Levenshtein (optimal string alignment) algorithm, though you can explicitly choose classic Levenshtein by
 * passing {@code false} to the {@code transpositions} parameter.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class FuzzyCondition extends SingleFieldCondition {

    /** The default Damerau-Levenshtein max distance. */
    public final static int DEFAULT_MAX_EDITS = LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE;

    /** The default length of common (non-fuzzy) prefix. */
    public final static int DEFAULT_PREFIX_LENGTH = 0;

    /** The default length of common (non-fuzzy) prefix. */
    public final static int DEFAULT_MAX_EXPANSIONS = 50;

    /** If transpositions should be treated as a primitive edit operation by default. */
    public final static boolean DEFAULT_TRANSPOSITIONS = true;

    /** The name of the field to be matched. */
    @JsonProperty("field")
    private final String field;

    /** The fuzzy expression to be matched. */
    @JsonProperty("value")
    private final String value;

    /** The Damerau-Levenshtein max distance. */
    @JsonProperty("max_edits")
    private final Integer maxEdits;

    /** The length of common (non-fuzzy) prefix. */
    @JsonProperty("prefix_length")
    private final Integer prefixLength;

    /** The length of common (non-fuzzy) prefix. */
    @JsonProperty("max_expansions")
    private final Integer maxExpansions;

    /** If transpositions should be treated as a primitive edit operation. */
    @JsonProperty("transpositions")
    private final Boolean transpositions;

    /**
     * Returns a new {@link FuzzyCondition}.
     *
     * @param boost          The boost for this query clause. Documents matching this clause will (in addition to the
     *                       normal weightings) have their score multiplied by {@code boost}. If {@code null}, then
     *                       {@link #DEFAULT_BOOST} is used as default.
     * @param field          The field name.
     * @param value          The field fuzzy value.
     * @param maxEdits       Must be >= 0 and <= {@link LevenshteinAutomata#MAXIMUM_SUPPORTED_DISTANCE}.
     * @param prefixLength   Length of common (non-fuzzy) prefix
     * @param maxExpansions  The maximum number of terms to match. If this number is greater than {@link
     *                       BooleanQuery#getMaxClauseCount} when the query is rewritten, then the maxClauseCount will
     *                       be used instead.
     * @param transpositions True if transpositions should be treated as a primitive edit operation. If this is false,
     *                       comparisons will implement the classic Levenshtein algorithm.
     */
    @JsonCreator
    public FuzzyCondition(@JsonProperty("boost") Float boost,
                          @JsonProperty("field") String field,
                          @JsonProperty("value") String value,
                          @JsonProperty("max_edits") Integer maxEdits,
                          @JsonProperty("prefix_length") Integer prefixLength,
                          @JsonProperty("max_expansions") Integer maxExpansions,
                          @JsonProperty("transpositions") Boolean transpositions) {
        super(boost);

        this.field = field;
        this.value = value;
        this.maxEdits = maxEdits == null ? DEFAULT_MAX_EDITS : maxEdits;
        this.prefixLength = prefixLength == null ? DEFAULT_PREFIX_LENGTH : prefixLength;
        this.maxExpansions = maxExpansions == null ? DEFAULT_MAX_EXPANSIONS : maxExpansions;
        this.transpositions = transpositions == null ? DEFAULT_TRANSPOSITIONS : transpositions;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(Schema schema) {

        if (field == null || field.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name required");
        }
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Field value required");
        }
        if (maxEdits < 0 || maxEdits > 2) {
            throw new IllegalArgumentException("max_edits must be between 0 and 2");
        }
        if (prefixLength < 0) {
            throw new IllegalArgumentException("prefix_length must be positive.");
        }
        if (maxExpansions < 0) {
            throw new IllegalArgumentException("max_expansions must be positive.");
        }

        ColumnMapperSingle<?> columnMapper = getMapper(schema, field);
        Class<?> clazz = columnMapper.baseClass();
        if (clazz == String.class) {
            String analyzedValue = analyze(field, value, schema);
            Term term = new Term(field, analyzedValue);
            Query query = new FuzzyQuery(term, maxEdits, prefixLength, maxExpansions, transpositions);
            query.setBoost(boost);
            return query;
        } else {
            String message = String.format("Fuzzy queries are not supported by %s mapper", clazz.getSimpleName());
            throw new UnsupportedOperationException(message);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("field", field)
                      .add("value", value)
                      .add("maxEdits", maxEdits)
                      .add("prefixLength", prefixLength)
                      .add("maxExpansions", maxExpansions)
                      .add("transpositions", transpositions)
                      .toString();
    }
}