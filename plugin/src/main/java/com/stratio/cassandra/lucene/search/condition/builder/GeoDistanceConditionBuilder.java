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

package com.stratio.cassandra.lucene.search.condition.builder;

import com.stratio.cassandra.lucene.common.GeoDistance;
import com.stratio.cassandra.lucene.search.condition.GeoDistanceCondition;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link ConditionBuilder} for building a new {@link GeoDistanceCondition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoDistanceConditionBuilder extends ConditionBuilder<GeoDistanceCondition, GeoDistanceConditionBuilder> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    private final String field;

    /** The latitude of the reference point. */
    @JsonProperty("latitude")
    private final double latitude;

    /** The longitude of the reference point. */
    @JsonProperty("longitude")
    private final double longitude;

    /** The max allowed distance. */
    @JsonProperty("max_distance")
    private final String maxDistance;

    /** The min allowed distance. */
    @JsonProperty("min_distance")
    private String minDistance;

    /**
     * Returns a new {@link GeoDistanceConditionBuilder} with the specified field reference point.
     *
     * @param field The name of the field to be matched.
     * @param latitude The latitude of the reference point.
     * @param longitude The longitude of the reference point.
     * @param maxDistance The max allowed distance.
     */
    @JsonCreator
    public GeoDistanceConditionBuilder(@JsonProperty("field") String field,
                                       @JsonProperty("latitude") double latitude,
                                       @JsonProperty("longitude") double longitude,
                                       @JsonProperty("max_distance") String maxDistance) {
        this.field = field;
        this.longitude = longitude;
        this.latitude = latitude;
        this.maxDistance = maxDistance;
    }

    /**
     * Sets the min allowed distance.
     *
     * @param minDistance the min allowed distance
     * @return this with the specified min distance
     */
    public GeoDistanceConditionBuilder setMinDistance(String minDistance) {
        this.minDistance = minDistance;
        return this;
    }

    /**
     * Returns the {@link GeoDistanceCondition} represented by this builder.
     *
     * @return a new geo distance condition
     */
    @Override
    public GeoDistanceCondition build() {
        GeoDistance min = minDistance == null ? null : GeoDistance.parse(minDistance);
        GeoDistance max = maxDistance == null ? null : GeoDistance.parse(maxDistance);
        return new GeoDistanceCondition(boost, field, latitude, longitude, min, max);
    }
}
