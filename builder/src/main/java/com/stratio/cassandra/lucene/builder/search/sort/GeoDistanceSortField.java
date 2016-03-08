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

package com.stratio.cassandra.lucene.builder.search.sort;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A geo spatial distance search sort.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoDistanceSortField extends SortField {

    /** The name of mapper to use to calculate distance. */
    @JsonProperty("mapper")
    final String mapper;

    /** The longitude of the center point to sort by distance to it. */
    @JsonProperty("longitude")
    final double longitude;

    /** The latitude of the center point to sort by distance to it. */
    @JsonProperty("latitude")
    final double latitude;

    /**
     * Creates a new {@link GeoDistanceSortField} for the specified field and reverse option.
     *
     * @param mapper the name of the field to be used for sort
     * @param longitude the longitude in degrees of the reference point
     * @param latitude the latitude in degrees of the reference point
     */
    @JsonCreator
    public GeoDistanceSortField(@JsonProperty("mapper") String mapper,
                                @JsonProperty("longitude") double longitude,
                                @JsonProperty("latitude") double latitude) {
        this.mapper = mapper;
        this.longitude = longitude;
        this.latitude = latitude;
    }
}