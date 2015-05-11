/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.query.builder;

import com.stratio.cassandra.lucene.query.Condition;
import com.stratio.cassandra.lucene.query.Search;
import com.stratio.cassandra.lucene.query.Sort;

/**
 * {@link Builder} for building a new {@link Search}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SearchBuilder implements Builder<Search> {

    /** he {@link Condition} for querying, maybe {@code null} meaning no querying. */
    private Condition queryCondition;

    /** The {@link Condition} for filtering, maybe {@code null} meaning no filtering. */
    private Condition filterCondition;

    /**
     * The {@link Sort} for the query. Note that is the order in which the data will be read before querying, not the
     * order of the results after querying.
     */
    private Sort sort;

    /**
     * Returns this builder with the specified querying condition.
     *
     * @param queryConditionBuilder The querying condition to be set.
     * @return This builder with the specified querying condition.
     */
    public SearchBuilder query(ConditionBuilder queryConditionBuilder) {
        this.queryCondition = queryConditionBuilder.build();
        return this;
    }

    /**
     * Returns this builder with the specified filtering condition.
     *
     * @param filterConditionBuilder The filtering condition to be set.
     * @return This builder with the specified filtering condition.
     */
    public SearchBuilder filter(ConditionBuilder filterConditionBuilder) {
        this.filterCondition = filterConditionBuilder.build();
        return this;
    }

    /**
     * Returns this builder with the specified sorting.
     *
     * @param sortFieldBuilders The sorting fields to be set.
     * @return This builder with the specified sorting.
     */
    public SearchBuilder sort(SortFieldBuilder... sortFieldBuilders) {
        this.sort = new SortBuilder(sortFieldBuilders).build();
        return this;
    }

    /**
     * Returns the {@link Search} represented by this builder.
     *
     * @return The {@link Search} represented by this builder.
     */
    @Override
    public Search build() {
        return new Search(queryCondition, filterCondition, sort);
    }

    /**
     * Returns the JSON representation of the {@link Search} represented by this builder.
     *
     * @return The JSON representation of the {@link Search} represented by this builder.
     */
    public String toJson() {
        return build().toJson();
    }

}
