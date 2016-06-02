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
import com.stratio.cassandra.lucene.search.sort.Sort;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

import java.util.List;
import java.util.Optional;

/**
 * Class representing an Lucene index search. It is formed by an optional querying {@link Condition} and an optional
 * filtering {@link Condition}. It can be translated to a Lucene {@link Query} using a {@link Schema}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Search {

    private static final boolean DEFAULT_FORCE_REFRESH = false;

    /** The {@link Condition} for querying, maybe {@code null} meaning no querying. */
    private final Condition query;

    /** The {@link Condition} for filtering, maybe {@code null} meaning no filtering. */
    private final Condition filter;

    /**
     * The {@link Sort} for the query. Note that is the order in which the data will be read before querying, not the
     * order of the results after querying.
     */
    private final Sort sort;

    /** If this search must refresh the index before reading it. */
    private final Boolean refresh;

    /** The paging state. */
    private final IndexPagingState paging;

    /**
     * Constructor using the specified querying, filtering, sorting and refresh options.
     *
     * @param query the condition for querying, maybe {@code null} meaning no querying
     * @param filter the condition for filtering, maybe {@code null} meaning no filtering
     * @param sort the sort for the query, maybe {@code null} meaning no sorting
     * @param paging the paging state
     * @param refresh if this search must refresh the index before reading it
     */
    public Search(Condition query,
                  Condition filter,
                  Sort sort,
                  IndexPagingState paging, Boolean refresh) {
        this.query = query;
        this.filter = filter;
        this.sort = sort;
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
        return query != null;
    }

    /**
     * Returns if this search uses field sorting.
     *
     * @return {@code true} if this search uses field sorting, {@code false} otherwise
     */
    public boolean usesSorting() {
        return sort != null;
    }

    /**
     * Returns if this search doesn't specify any filter, query or sort.
     *
     * @return {@code true} if this search doesn't specify any filter, query or sort, {@code false} otherwise
     */
    public boolean isEmpty() {
        return query == null && filter == null && sort == null;
    }

    /**
     * Returns the field sorting to be used, maybe {@code null} meaning no field sorting.
     *
     * @return the Lucene's sort
     */
    public Sort getSort() {
        return this.sort;
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
     * Returns the Lucene {@link SortField}s represented by this using the specified {@link Schema}. Maybe {@code null}
     * meaning no sorting.
     *
     * @param schema the {@link Schema}
     * @return the Lucene {@link SortField}s represented by this using {@code schema}
     */
    public List<SortField> sortFields(Schema schema) {
        return sort == null ? null : sort.sortFields(schema);
    }

    /**
     * Returns the Lucene filtering {@link Query} represented by this using the specified {@link Schema}.
     *
     * @param schema the {@link Schema}
     * @return a Lucene {@link Query}
     */
    public Optional<Query> filter(Schema schema) {
        return filter == null ? Optional.empty() : Optional.of(filter.query(schema));
    }

    /**
     * Returns the Lucene {@link Query} represented by this using the specified {@link Schema}.
     *
     * @param schema the {@link Schema}
     * @return a Lucene {@link Query}
     */
    public Optional<Query> query(Schema schema) {
        return query == null ? Optional.empty() : Optional.of(query.query(schema));
    }

    public IndexPagingState paging() {
        return paging;
    }

    /**
     * Validates this {@link Search} against the specified {@link Schema}.
     *
     * @param schema a {@link Schema}
     */
    public void validate(Schema schema) {
        if (query != null) {
            query.query(schema);
        }
        if (filter != null) {
            filter.query(schema);
        }
        if (sort != null) {
            sort.sortFields(schema);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("query", query)
                          .add("filter", filter)
                          .add("sort", sort)
                          .add("refresh", refresh)
                          .add("paging", paging)
                          .toString();
    }
}
