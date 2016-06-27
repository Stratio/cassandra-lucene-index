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

import com.stratio.cassandra.lucene.IndexException;
import com.stratio.cassandra.lucene.IndexPagingState;
import com.stratio.cassandra.lucene.search.condition.builder.ConditionBuilder;
import com.stratio.cassandra.lucene.search.sort.builder.SortFieldBuilder;
import com.stratio.cassandra.lucene.util.ByteBufferUtils;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;

/**
 * Class for building a new {@link Search} using the old syntax prior to 3.0.7.2.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@Deprecated
@SuppressWarnings("unused")
class SearchBuilderLegacy {

    private SearchBuilder builder = new SearchBuilder();

    /**
     * Sets the specified filtering condition.
     *
     * @param filterBuilder the filtering condition to be set
     */
    @JsonProperty("filter")
    void filter(ConditionBuilder<?, ?> filterBuilder) {
        builder.filter(filterBuilder);
    }

    /**
     * Sets the specified querying condition.
     *
     * @param queryBuilder the querying condition to be set
     */
    @JsonProperty("query")
    void query(ConditionBuilder<?, ?> queryBuilder) {
        builder.query(queryBuilder);
    }

    /**
     * Sets the specified sorting.
     *
     * @param sortBuilder the sorting fields to be set
     */
    @JsonProperty("sort")
    void sort(SortBuilder sortBuilder) {
        builder.sort(sortBuilder.fields);
    }

    /**
     * Sets if the {@link Search} to be built must refresh the index before reading it. Refresh is a costly operation so
     * you should use it only when it is strictly required.
     *
     * @param refresh {@code true} if the {@link Search} to be built must refresh the Lucene's index searcher before
     * searching, {@code false} otherwise
     */
    @JsonProperty("refresh")
    void refresh(boolean refresh) {
        builder.refresh(refresh);
    }

    /**
     * Sets the specified starting partition key.
     *
     * @param paging a paging state
     */
    @JsonProperty("paging")
    void paging(String paging) {
        builder.paging(IndexPagingState.build(ByteBufferUtils.byteBuffer(paging)));
    }

    /**
     * Returns the {@link SearchBuilder} represented by the specified JSON {@code String}.
     *
     * @param json the JSON {@code String} representing a {@link SearchBuilder}
     * @return the {@link SearchBuilder} represented by the specified JSON {@code String}
     */
    static SearchBuilder fromJson(String json) {
        try {
            return JsonSerializer.fromString(json, SearchBuilderLegacy.class).builder;
        } catch (IOException e) {
            throw new IndexException(e, "Unparseable JSON search: {}", json);
        }
    }

    private static final class SortBuilder {
        @JsonProperty("fields")
        SortFieldBuilder[] fields = new SortFieldBuilder[]{};
    }

}
