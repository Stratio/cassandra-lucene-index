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

package com.stratio.cassandra.lucene.builder.search;

import com.stratio.cassandra.lucene.builder.Builder;
import com.stratio.cassandra.lucene.builder.search.condition.Condition;
import com.stratio.cassandra.lucene.builder.search.sort.Sort;
import com.stratio.cassandra.lucene.builder.search.sort.SortField;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Class representing an Lucene index search. It is formed by an optional querying {@link Condition} and an optional
 * filtering {@link Condition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Search extends Builder {

    /** The {@link Condition} for querying, maybe {@code null} meaning no querying. */
    @JsonProperty("query")
    public Condition query;

    /** The {@link Condition} for filtering, maybe {@code null} meaning no filtering. */
    @JsonProperty("filter")
    public Condition filter;

    /** The {@link Sort} for the query, maybe {@code null} meaning no filtering. */
    @JsonProperty("sort")
    public Sort sort;

    /** If this search must force the refresh the index before reading it. */
    @JsonProperty("refresh")
    public Boolean refresh;

    /** Default constructor. */
    public Search() {
    }

    /**
     * Sets the querying condition. The specified condition will be taken into account in the relevance scoring.
     *
     * @param query the querying condition to be set
     * @return this with the specified query
     */
    public Search query(Condition query) {
        this.query = query;
        return this;
    }

    /**
     * Sets the filtering condition. The specified condition won't be taken into account in the relevance scoring.
     *
     * @param filter the filtering condition to be set
     * @return this with the specified filter
     */
    public Search filter(Condition filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Sets the sorting fields.
     *
     * @param sortFields the sorting fields to be set
     * @return this with the specified sort
     */
    public Search sort(SortField... sortFields) {
        this.sort = new Sort(sortFields);
        return this;
    }

    /**
     * Sets if the {@link Search} must refresh the Lucene's index searcher before using it. Refresh is a costly
     * operation so you should use it only when it is strictly required.
     *
     * @param refresh if the {@link Search} must refresh the index before reading it
     * @return this with the specified refresh
     */
    public Search refresh(Boolean refresh) {
        this.refresh = refresh;
        return this;
    }

}
