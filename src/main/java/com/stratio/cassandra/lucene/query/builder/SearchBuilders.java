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

import java.util.List;

/**
 * Factory for {@link SearchBuilder} and {@link ConditionBuilder}s.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class SearchBuilders {

    /**
     * Returns a new {@link SearchBuilder}.
     *
     * @return a new {@link SearchBuilder}.
     */
    public static SearchBuilder search() {
        return new SearchBuilder();
    }

    /**
     * Returns a new {@link SearchBuilder} using the specified {@link ConditionBuilder} as query.
     *
     * @return a new {@link SearchBuilder} using the specified {@link ConditionBuilder} as query.
     */
    public static SearchBuilder query(ConditionBuilder<?, ?> queryConditionBuilder) {
        return search().query(queryConditionBuilder);
    }

    /**
     * Returns a new {@link SearchBuilder} using the specified {@link ConditionBuilder} as clusteringKeyFilter.
     *
     * @return a new {@link SearchBuilder} using the specified {@link ConditionBuilder} as clusteringKeyFilter.
     */
    public static SearchBuilder filter(ConditionBuilder<?, ?> filterConditionBuilder) {
        return search().filter(filterConditionBuilder);
    }

    /**
     * Returns a new {@link SearchBuilder} using the specified {@link SortFieldBuilder}s as sorting.
     *
     * @return a new {@link SearchBuilder} using the specified {@link SortFieldBuilder}s as sorting.
     */
    public static SearchBuilder sort(SortFieldBuilder... sortFieldBuilders) {
        return search().sort(sortFieldBuilders);
    }

    /**
     * Returns a new {@link BooleanConditionBuilder}.
     *
     * @return A new {@link BooleanConditionBuilder}.
     */
    public static BooleanConditionBuilder bool() {
        return new BooleanConditionBuilder();
    }

    /**
     * Returns a new {@link FuzzyConditionBuilder} for the specified field and value.
     *
     * @param field The name of the field to be matched.
     * @param value The value of the field to be matched.
     * @return A new {@link FuzzyConditionBuilder} for the specified field and value.
     */
    public static FuzzyConditionBuilder fuzzy(String field, String value) {
        return new FuzzyConditionBuilder(field, value);
    }

    /**
     * Returns a new {@link LuceneConditionBuilder} with the specified query.
     *
     * @param query The Lucene syntax query.
     * @return A new {@link LuceneConditionBuilder} with the specified query.
     */
    public static LuceneConditionBuilder lucene(String query) {
        return new LuceneConditionBuilder(query);
    }

    /**
     * Returns a new {@link MatchConditionBuilder} for the specified field and value.
     *
     * @param field The name of the field to be matched.
     * @param value The value of the field to be matched.
     * @return A new {@link MatchConditionBuilder} for the specified field and value.
     */
    public static MatchConditionBuilder match(String field, Object value) {
        return new MatchConditionBuilder(field, value);
    }

    /**
     * Returns a new {@link MatchAllConditionBuilder} for the specified field and value.
     *
     * @return A new {@link MatchAllConditionBuilder} for the specified field and value.
     */
    public static MatchAllConditionBuilder matchAll() {
        return new MatchAllConditionBuilder();
    }

    /**
     * Returns a new {@link PhraseConditionBuilder} for the specified field and values.
     *
     * @param field  The name of the field to be matched.
     * @param values The values of the field to be matched.
     * @return A new {@link PhraseConditionBuilder} for the specified field and values.
     */
    public static PhraseConditionBuilder phrase(String field, String... values) {
        return new PhraseConditionBuilder(field, values);
    }

    /**
     * Returns a new {@link PhraseConditionBuilder} for the specified field and values.
     *
     * @param field  The name of the field to be matched.
     * @param values The values of the field to be matched.
     * @return A new {@link PhraseConditionBuilder} for the specified field and values.
     */
    public static PhraseConditionBuilder phrase(String field, List<String> values) {
        return new PhraseConditionBuilder(field, values);
    }

    /**
     * Returns a new {@link PrefixConditionBuilder} for the specified field and value.
     *
     * @param field The name of the field to be matched.
     * @param value The value of the field to be matched.
     * @return A new {@link PrefixConditionBuilder} for the specified field and value.
     */
    public static PrefixConditionBuilder prefix(String field, String value) {
        return new PrefixConditionBuilder(field, value);
    }

    /**
     * Returns a new {@link RangeConditionBuilder} for the specified field.
     *
     * @param field The name of the field to be matched.
     * @return A new {@link RangeConditionBuilder} for the specified field.
     */
    public static RangeConditionBuilder range(String field) {
        return new RangeConditionBuilder(field);
    }

    /**
     * Returns a new {@link RegexpConditionBuilder} for the specified field and value.
     *
     * @param field The name of the field to be matched.
     * @param value The value of the field to be matched.
     * @return A new {@link RegexpConditionBuilder} for the specified field and value.
     */
    public static RegexpConditionBuilder regexp(String field, String value) {
        return new RegexpConditionBuilder(field, value);
    }

    /**
     * Returns a new {@link WildcardConditionBuilder} for the specified field and value.
     *
     * @param field The name of the field to be matched.
     * @param value The value of the field to be matched.
     * @return A new {@link WildcardConditionBuilder} for the specified field and value.
     */
    public static WildcardConditionBuilder wildcard(String field, String value) {
        return new WildcardConditionBuilder(field, value);
    }

    /**
     * Returns a new {@link SortFieldBuilder} for the specified field.
     *
     * @param field The name of the field to be sorted.
     * @return A new {@link SortFieldBuilder} for the specified field.
     */
    public static SortFieldBuilder sortField(String field) {
        return new SortFieldBuilder(field);
    }
}
