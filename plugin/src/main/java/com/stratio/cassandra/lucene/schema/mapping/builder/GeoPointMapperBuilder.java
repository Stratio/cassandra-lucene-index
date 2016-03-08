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

package com.stratio.cassandra.lucene.schema.mapping.builder;

import com.stratio.cassandra.lucene.schema.mapping.GeoPointMapper;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * {@link MapperBuilder} to build a new {@link GeoPointMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoPointMapperBuilder extends MapperBuilder<GeoPointMapper, GeoPointMapperBuilder> {

    /** The name of the column containing the latitude. */
    @JsonProperty("latitude")
    private final String latitude;

    /** The name of the column containing the longitude. */
    @JsonProperty("longitude")
    private final String longitude;

    /** The maximum number of levels in the tree. */
    @JsonProperty("max_levels")
    private Integer maxLevels;

    /** The name of the column containing the longitude. */
    @JsonProperty("indexed")
    private Boolean indexed;

    /** The maximum number of levels in the tree. */
    @JsonProperty("sorted")
    private Boolean sorted;

    /**
     * Builds a new {@link GeoPointMapper}.
     *
     * @param latitude the name of the column containing the latitude
     * @param longitude the name of the column containing the longitude
     */
    @JsonCreator
    public GeoPointMapperBuilder(@JsonProperty("latitude") String latitude,
                                 @JsonProperty("longitude") String longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    /**
     * Sets the maximum number of levels in the tree.
     *
     * @param maxLevels the maximum number of levels in the tree
     * @return this
     */
    public GeoPointMapperBuilder maxLevels(Integer maxLevels) {
        this.maxLevels = maxLevels;
        return this;
    }

    /**
     * Sets if the field supports searching.
     *
     * @param indexed if the field supports searching
     * @return this
     */
    @SuppressWarnings("unchecked")
    public final GeoPointMapperBuilder indexed(Boolean indexed) {
        this.indexed = indexed;
        return this;
    }

    /**
     * Sets if the field supports sorting.
     *
     * @param sorted if the field supports sorting
     * @return this
     */
    @SuppressWarnings("unchecked")
    public final GeoPointMapperBuilder sorted(Boolean sorted) {
        this.sorted = sorted;
        return this;
    }

    /**
     * Returns the {@link GeoPointMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link GeoPointMapper} represented by this
     */
    @Override
    public GeoPointMapper build(String field) {
        return new GeoPointMapper(field, indexed, sorted, validated, latitude, longitude, maxLevels);
    }
}
