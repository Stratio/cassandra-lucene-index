/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.cassandra.lucene.search;

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.search.condition.Condition;
import com.stratio.cassandra.lucene.search.condition.builder.ConditionBuilder;
import com.stratio.cassandra.lucene.search.sort.Sort;
import com.stratio.cassandra.lucene.search.sort.builder.SortBuilder;
import com.stratio.cassandra.lucene.search.sort.builder.SortFieldBuilder;
import com.stratio.cassandra.lucene.util.Builder;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;

/**
 * Class for building a new {@link Search}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SearchBuilder implements Builder<Search> {

    /** The {@link Condition} for querying, maybe {@code null} meaning no querying. */
    @JsonProperty("query")
    private ConditionBuilder<?, ?> queryBuilder;

    /** The {@link Condition} for filtering, maybe {@code null} meaning no filtering. */
    @JsonProperty("filter")
    private ConditionBuilder<?, ?> filterBuilder;

    /** The {@link Sort} for the query, maybe {@code null} meaning no filtering. */
    @JsonProperty("sort")
    private SortBuilder sortBuilder;

    /** If this search must force the refresh the index before reading it. */
    @JsonProperty("refresh")
    private boolean refresh;

    /** Default constructor. */
    public SearchBuilder() {
    }

    /**
     * Sets the specified querying condition.
     *
     * @param queryBuilder the querying condition to be set
     * @return this builder with the specified querying condition
     */
    public SearchBuilder query(ConditionBuilder<?, ?> queryBuilder) {
        this.queryBuilder = queryBuilder;
        return this;
    }

    /**
     * Sets the specified filtering condition.
     *
     * @param filterBuilder the filtering condition to be set
     * @return this builder with the specified filtering condition
     */
    public SearchBuilder filter(ConditionBuilder<?, ?> filterBuilder) {
        this.filterBuilder = filterBuilder;
        return this;
    }

    /**
     * Sets the specified sorting.
     *
     * @param sortBuilder The sorting fields to be set.
     * @return this builder with the specified sort
     */
    public SearchBuilder sort(SortBuilder sortBuilder) {
        this.sortBuilder = sortBuilder;
        return this;
    }

    /**
     * Sets the specified sorting.
     *
     * @param sortFieldBuilders The sorting fields to be set.
     * @return this builder with the specified sorting
     */
    public SearchBuilder sort(SortFieldBuilder... sortFieldBuilders) {
        this.sortBuilder = new SortBuilder(sortFieldBuilders);
        return this;
    }

    /**
     * Sets if the {@link Search} to be built must refresh the index before reading it. Refresh is a costly operation so
     * you should use it only when it is strictly required.
     *
     * @param refresh {@code true} if the {@link Search} to be built must refresh the Lucene's index searcher before
     * searching, {@code false} otherwise
     * @return this builder with the specified refresh
     */
    public SearchBuilder refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    /**
     * Returns the {@link Search} represented by this builder.
     *
     * @return the search represented by this builder
     */
    public Search build() {
        Condition query = queryBuilder == null ? null : queryBuilder.build();
        Condition filter = filterBuilder == null ? null : filterBuilder.build();
        Sort sort = sortBuilder == null ? null : sortBuilder.build();
        return new Search(query, filter, sort, refresh);
    }

    /**
     * Returns the JSON representation of this object.
     *
     * @return a JSON representation of this object
     */
    public String toJson() {
        build();
        try {
            return JsonSerializer.toString(this);
        } catch (IOException e) {
            throw new IndexException(e, "Unformateable JSON search: %s", e.getMessage());
        }
    }

    /**
     * Returns the {@link SearchBuilder} represented by the specified JSON {@code String}.
     *
     * @param json the JSON {@code String} representing a {@link SearchBuilder}
     * @return the {@link SearchBuilder} represented by the specified JSON {@code String}
     */
    public static SearchBuilder fromJson(String json) {
        try {
            return JsonSerializer.fromString(json, SearchBuilder.class);
        } catch (IOException e) {
            throw new IndexException(e, "Unparseable JSON search: %s", e.getMessage());
        }
    }

}
