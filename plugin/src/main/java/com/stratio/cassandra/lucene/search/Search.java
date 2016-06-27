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
package com.stratio.cassandra.lucene.search;

import com.google.common.base.MoreObjects;
import com.stratio.cassandra.lucene.IndexPagingState;
import com.stratio.cassandra.lucene.schema.Schema;
import com.stratio.cassandra.lucene.search.condition.Condition;
import com.stratio.cassandra.lucene.search.sort.SortField;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;

/**
 * Class representing an Lucene index search. It can be translated to a Lucene {@link Query} using a {@link Schema}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Search {

    protected static final Logger logger = LoggerFactory.getLogger(Search.class);

    private static final boolean DEFAULT_FORCE_REFRESH = false;

    /** The mandatory conditions not participating in scoring. */
    public final List<Condition> filter;

    /** The mandatory conditions participating in scoring. */
    public final List<Condition> query;

    /** The sorting fields for the query. */
    private final List<SortField> sort;

    /** If this search must refresh the index before reading it. */
    private final Boolean refresh;

    /** The paging state. */
    private final IndexPagingState paging;

    /**
     * Constructor using the specified querying, filtering, sorting and refresh options.
     *
     * @param filter the filtering {@link Condition}s not involved in scoring
     * @param query the querying {@link Condition}s participating in scoring
     * @param sort the sort fields for the query
     * @param paging the paging state
     * @param refresh if this search must refresh the index before reading it
     */
    public Search(List<Condition> filter,
                  List<Condition> query,
                  List<SortField> sort,
                  IndexPagingState paging,
                  Boolean refresh) {
        this.filter = filter == null ? Collections.EMPTY_LIST : filter;
        this.query = query == null ? Collections.EMPTY_LIST : query;
        this.sort = sort == null ? Collections.EMPTY_LIST : sort;
        this.paging = paging;
        this.refresh = refresh == null ? DEFAULT_FORCE_REFRESH : refresh;
    }

    /**
     * Returns if this search requires post reconciliation agreement processing to preserve the order of its results.
     *
     * @return {@code true} if it requires post processing, {@code false} otherwise
     */
    public boolean requiresPostProcessing() {
        return usesRelevance() || usesSorting();
    }

    /**
     * Returns if this search requires full ranges scan.
     *
     * @return {@code true} if this search requires full ranges scan, {code null} otherwise
     */
    public boolean requiresFullScan() {
        return usesRelevance() || usesSorting() || refresh && isEmpty();
    }

    /**
     * Returns if this search uses Lucene relevance formula.
     *
     * @return {@code true} if this search uses Lucene relevance formula, {@code false} otherwise
     */
    public boolean usesRelevance() {
        return !query.isEmpty();
    }

    /**
     * Returns if this search uses field sorting.
     *
     * @return {@code true} if this search uses field sorting, {@code false} otherwise
     */
    public boolean usesSorting() {
        return !sort.isEmpty();
    }

    /**
     * Returns if this search doesn't specify any filter, query or sort.
     *
     * @return {@code true} if this search doesn't specify any filter, query or sort, {@code false} otherwise
     */
    public boolean isEmpty() {
        return filter.isEmpty() && query.isEmpty() && sort.isEmpty();
    }

    /**
     * Returns the Lucene {@link Query} represented by this search, with the additional optional data range filter.
     *
     * @param schema the indexing schema
     * @param range the additional data range filter, maybe {@code null}
     * @return a Lucene {@link Query}
     */
    public Query query(Schema schema, Query range) {

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        if (range != null) {
            builder.add(range, FILTER);
        }

        filter.forEach(condition -> builder.add(condition.query(schema), FILTER));
        query.forEach(condition -> builder.add(condition.query(schema), MUST));

        BooleanQuery booleanQuery = builder.build();
        return booleanQuery.clauses().isEmpty() ? new MatchAllDocsQuery() : booleanQuery;
    }

    public Query postProcessingQuery(Schema schema) {
        if (query.isEmpty()) {
            return new MatchAllDocsQuery();
        } else {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            query.forEach(condition -> builder.add(condition.query(schema), MUST));
            return builder.build();
        }
    }

    /**
     * Returns if this search needs to refresh the index before reading it.
     *
     * @return {@code true} if this search needs to refresh the index before reading it, {@code false} otherwise.
     */
    public boolean refresh() {
        return refresh;
    }

    /**
     * Returns the Lucene {@link org.apache.lucene.search.SortField}s represented by this using the specified schema.
     *
     * @param schema the indexing schema to be used
     * @return the Lucene sort fields represented by this using {@code schema}
     */
    public List<org.apache.lucene.search.SortField> sortFields(Schema schema) {
        return sort.stream().map(s -> s.sortField(schema)).collect(Collectors.toList());
    }

    public IndexPagingState paging() {
        return paging;
    }

    /**
     * Returns the names of the involved fields when post processing.
     *
     * @return the names of the involved fields
     */
    public Set<String> postProcessingFields() {
        Set<String> fields = new LinkedHashSet<>();
        query.forEach(condition -> fields.addAll(condition.postProcessingFields()));
        sort.forEach(condition -> fields.addAll(condition.postProcessingFields()));
        return fields;
    }

    /**
     * Validates this {@link Search} against the specified {@link Schema}.
     *
     * @param schema a {@link Schema}
     */
    public void validate(Schema schema) {
        filter.forEach(condition -> condition.query(schema));
        query.forEach(condition -> condition.query(schema));
        sort.forEach(field -> field.sortField(schema));
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("filter", filter)
                          .add("query", query)
                          .add("sort", sort)
                          .add("refresh", refresh)
                          .add("paging", paging)
                          .toString();
    }
}
