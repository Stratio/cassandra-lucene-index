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

import com.stratio.cassandra.lucene.search.condition.builder.*;
import com.stratio.cassandra.lucene.search.sort.builder.SortFieldBuilder;

/**
 * Factory for {@link SearchBuilder} and {@link ConditionBuilder}s.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class SearchBuilders {

    /** Private constructor to hide the implicit public one. */
    private SearchBuilders() {
    }

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
     * @param conditionBuilder The {@link ConditionBuilder} containing the query.
     * @return a new {@link SearchBuilder} using the specified {@link ConditionBuilder} as query.
     */
    public static SearchBuilder query(ConditionBuilder<?, ?> conditionBuilder) {
        return search().query(conditionBuilder);
    }

    /**
     * Returns a new {@link SearchBuilder} using the specified {@link ConditionBuilder} as clusteringKeyFilter.
     *
     * @param conditionBuilder The {@link ConditionBuilder} containing the filter.
     * @return a new {@link SearchBuilder} using the specified {@link ConditionBuilder} as clusteringKeyFilter.
     */
    public static SearchBuilder filter(ConditionBuilder<?, ?> conditionBuilder) {
        return search().filter(conditionBuilder);
    }

    /**
     * Returns a new {@link SearchBuilder} using the specified {@link SortFieldBuilder}s as sorting.
     *
     * @param sortFieldBuilders The {@link SortFieldBuilder}s.
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
     * Returns a new {@link AllConditionBuilder} for the specified field and value.
     *
     * @return A new {@link AllConditionBuilder} for the specified field and value.
     */
    public static AllConditionBuilder all() {
        return new AllConditionBuilder();
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
     * Returns a new {@link NoneConditionBuilder} for the specified field and value.
     *
     * @return A new {@link NoneConditionBuilder} for the specified field and value.
     */
    public static NoneConditionBuilder none() {
        return new NoneConditionBuilder();
    }

    /**
     * Returns a new {@link PhraseConditionBuilder} for the specified field and values.
     *
     * @param field The name of the field to be matched.
     * @param value The text to be matched.
     * @return A new {@link PhraseConditionBuilder} for the specified field and values.
     */
    public static PhraseConditionBuilder phrase(String field, String value) {
        return new PhraseConditionBuilder(field, value);
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
     * Returns a new {@link GeoBBoxConditionBuilder} with the specified field name and bounding box coordinates.
     *
     * @param field        The name of the field to be matched.
     * @param minLongitude The minimum accepted longitude.
     * @param maxLongitude The maximum accepted longitude.
     * @param minLatitude  The minimum accepted latitude.
     * @param maxLatitude  The maximum accepted latitude.
     * @return A new {@link GeoBBoxConditionBuilder}.
     */
    public static GeoBBoxConditionBuilder geoBBox(String field,
                                                  double minLongitude,
                                                  double maxLongitude,
                                                  double minLatitude,
                                                  double maxLatitude) {
        return new GeoBBoxConditionBuilder(field, minLatitude, maxLatitude, minLongitude, maxLongitude);
    }

    /**
     * Returns a new {@link GeoDistanceConditionBuilder} with the specified field reference point.
     *
     * @param field       The name of the field to be matched.
     * @param longitude   The longitude of the reference point.
     * @param latitude    The latitude of the reference point.
     * @param maxDistance The max allowed distance.
     * @return A new {@link GeoDistanceConditionBuilder}.
     */
    public static GeoDistanceConditionBuilder geoDistance(String field,
                                                          double longitude,
                                                          double latitude,
                                                          String maxDistance) {
        return new GeoDistanceConditionBuilder(field, latitude, longitude, maxDistance);
    }

    /**
     * Returns a new {@link DateRangeConditionBuilder} with the specified field reference point.
     *
     * @param field The name of the field to be matched.
     * @return A new {@link DateRangeConditionBuilder}.
     */
    public static DateRangeConditionBuilder dateRange(String field) {
        return new DateRangeConditionBuilder(field);
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

    /**
     * Returns a new {@link BitemporalConditionBuilder} for the specified field.
     *
     * @param field The name of the field to be sorted.
     * @return A new {@link BitemporalConditionBuilder} for the specified field.
     */
    public static BitemporalConditionBuilder bitemporalSearch(String field) {
        return new BitemporalConditionBuilder(field);
    }
}
