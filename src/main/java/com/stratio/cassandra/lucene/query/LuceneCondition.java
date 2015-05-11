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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A {@link Condition} implementation that matches documents satisfying a Lucene Query Syntax.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class LuceneCondition extends Condition {

    /** The default name of the field where the clauses will be applied by default. */
    public static final String DEFAULT_FIELD = "lucene";

    /** The Lucene query syntax expression. */
    @JsonProperty("query")
    private final String query;

    /** The name of the field where the clauses will be applied by default. */
    @JsonProperty("default_field")
    private String defaultField;

    /**
     * Constructor using the field name and the value to be matched.
     *
     * @param boost        The boost for this query clause. Documents matching this clause will (in addition to the
     *                     normal weightings) have their score multiplied by {@code boost}. If {@code null}, then {@link
     *                     #DEFAULT_BOOST} is used as default.
     * @param defaultField the default field name.
     * @param query        the Lucene Query Syntax query.
     */
    @JsonCreator
    public LuceneCondition(@JsonProperty("boost") Float boost,
                           @JsonProperty("default_field") String defaultField,
                           @JsonProperty("query") String query) {
        super(boost);

        this.query = query;
        this.defaultField = defaultField == null ? DEFAULT_FIELD : defaultField;
    }

    /** {@inheritDoc} */
    @Override
    public Query query(Schema schema) {

        if (query == null) {
            throw new IllegalArgumentException("Query statement required");
        }

        try {
            Analyzer analyzer = schema.getAnalyzer();
            QueryParser queryParser = new QueryParser(defaultField, analyzer);
            queryParser.setAllowLeadingWildcard(true);
            queryParser.setLowercaseExpandedTerms(false);
            Query luceneQuery = queryParser.parse(query);
            luceneQuery.setBoost(boost);
            return luceneQuery;
        } catch (ParseException e) {
            throw new RuntimeException("Error while parsing lucene syntax query", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("query", query).add("defaultField", defaultField).toString();
    }
}