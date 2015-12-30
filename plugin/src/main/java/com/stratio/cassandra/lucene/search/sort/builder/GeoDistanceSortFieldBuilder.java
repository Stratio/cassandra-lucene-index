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

package com.stratio.cassandra.lucene.search.sort.builder;

import com.stratio.cassandra.lucene.search.sort.GeoDistanceSortField;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class GeoDistanceSortFieldBuilder extends SortFieldBuilder<GeoDistanceSortField,GeoDistanceSortFieldBuilder> {

    /** The longitude of the center point to sort by min distance to it. */
    @JsonProperty("longitude")
    private final double longitude;

    /** The latitude of the center point to sort by min distance to it. */
    @JsonProperty("latitude")
    private final double latitude;

    /**
     * Creates a new {@link GeoDistanceSortFieldBuilder} for the specified field.
     *
     * @param field     The field to sort by.
     * @param longitude The longitude of the center point to sort by min distance to it.
     * @param latitude  The latitude of the center point to sort by min distance to it.
     */
    @JsonCreator
    public GeoDistanceSortFieldBuilder(@JsonProperty("field") String field,
                                       @JsonProperty("longitude") double longitude, @JsonProperty("latitude") double latitude) {


        super(field);
        this.longitude=longitude;
        this.latitude=latitude;
    }

    /** {@inheritDoc} */
    @Override
    public GeoDistanceSortField build() {
        return new GeoDistanceSortField(field, reverse,longitude,latitude);
    }
}
