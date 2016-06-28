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
package com.stratio.cassandra.lucene.builder.search;

import com.stratio.cassandra.lucene.builder.JSONBuilder;
import com.stratio.cassandra.lucene.builder.search.condition.Condition;
import com.stratio.cassandra.lucene.builder.search.sort.Sort;
import com.stratio.cassandra.lucene.builder.search.sort.SortField;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 * Class representing an Lucene index search. It is formed by an optional querying {@link Condition} and an optional
 * filtering {@link Condition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@SuppressWarnings("unused")
public class Search extends JSONBuilder {

    /** The filtering conditions not participating in scoring. */
    @JsonProperty("filter")
    private List<Condition> filter;

    /** The querying conditions participating in scoring. */
    @JsonProperty("query")
    private List<Condition> query;

    /** The {@link Sort} for the query, maybe {@code null} meaning no filtering. */
    @JsonProperty("sort")
    private List<SortField> sort;

    /** If this search must force the refresh the index before reading it. */
    @JsonProperty("refresh")
    private Boolean refresh;

    /** Default constructor. */
    public Search() {
    }

    /**
     * Returns this with the specified filtering conditions not participating in scoring.
     *
     * @param conditions the filtering conditions to be added
     * @return this with the specified filtering conditions
     */
    public Search filter(Condition... conditions) {
        filter = add(filter, conditions);
        return this;
    }

    /**
     * Returns this with the specified querying conditions participating in scoring.
     *
     * @param conditions the mandatory conditions to be added
     * @return this with the specified mandatory conditions
     */
    public Search query(Condition... conditions) {
        query = add(query, conditions);
        return this;
    }

    /**
     * Sets the sorting fields.
     *
     * @param fields the sorting fields to be added
     * @return this with the specified sorting fields
     */
    public Search sort(SortField... fields) {
        sort = add(sort, fields);
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
