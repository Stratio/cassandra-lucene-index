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
package com.stratio.cassandra.lucene.query.builder;

import com.stratio.cassandra.lucene.query.GeoBBoxCondition;
import com.stratio.cassandra.lucene.query.GeoDistanceCondition;
import com.stratio.cassandra.lucene.util.GeoDistance;

/**
 * {@link ConditionBuilder} for building a new {@link GeoBBoxCondition}.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoDistanceConditionBuilder extends ConditionBuilder<GeoDistanceCondition, GeoDistanceConditionBuilder> {

    private String field; // The name of the field to be matched.
    private double longitude; // The longitude of the reference point.
    private double latitude; // The latitude of the reference point.
    private String minDistance; // The min allowed distance.
    private String maxDistance; // The max allowed distance.

    /**
     * Returns a new {@link GeoDistanceConditionBuilder} with the specified field reference point.
     *
     * @param field     The name of the field to be matched.
     * @param longitude The longitude of the reference point.
     * @param latitude  The latitude of the reference point.
     */
    public GeoDistanceConditionBuilder(String field, double longitude, double latitude) {
        this.field = field;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    /**
     * Sets the min allowed {@link GeoDistance}.
     *
     * @param minDistance The min allowed {@link GeoDistance}.
     * @return This.
     */
    public GeoDistanceConditionBuilder setMinDistance(String minDistance) {
        this.minDistance = minDistance;
        return this;
    }

    /**
     * Sets the max allowed {@link GeoDistance}.
     *
     * @param maxDistance The max allowed {@link GeoDistance}.
     * @return This.
     */
    public GeoDistanceConditionBuilder setMaxDistance(String maxDistance) {
        this.maxDistance = maxDistance;
        return this;
    }

    /**
     * Returns the {@link GeoDistanceCondition} represented by this builder.
     *
     * @return The {@link GeoDistanceCondition} represented by this builder.
     */
    @Override
    public GeoDistanceCondition build() {
        return new GeoDistanceCondition(boost, field, longitude, latitude, minDistance, maxDistance);
    }
}
