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
package com.stratio.cassandra.lucene.builder.index.schema.mapping;

import com.stratio.cassandra.lucene.builder.common.GeoTransformation;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.List;

/**
 * A {@link Mapper} to map geographical points.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoShapeMapper extends Mapper<GeoShapeMapper> {

    /** The name of the column to be mapped. */
    @JsonProperty("column")
    String column;

    /** The maximum number of levels in the geohash search tree. */
    @JsonProperty("max_levels")
    Integer maxLevels;

    /** The sequence of transformations to be applied to the shape before indexing it. */
    @JsonProperty("transformations")
    private List<GeoTransformation> transformations;

    /**
     * Sets the name of the Cassandra column to be mapped.
     *
     * @param column the name of the Cassandra column to be mapped
     * @return this with the specified column
     */
    public final GeoShapeMapper column(String column) {
        this.column = column;
        return this;
    }

    /**
     * Sets the maximum number of levels in the geohash search tree. False positives will be discarded using stored doc
     * values, so a low value doesn't mean precision lost. High values will produce few false positives to
     * be post-filtered, at the expense of creating many terms in the search index. This can be specially costly in
     * large polygons, which can produce thousands of terms in case of using large geohash values.
     *
     * @param maxLevels the maximum number of levels in the geohash search tree
     * @return this with the specified max number of levels
     */
    public GeoShapeMapper maxLevels(Integer maxLevels) {
        this.maxLevels = maxLevels;
        return this;
    }

    /**
     * Sets the transformations to be applied to the shape before indexing it.
     *
     * @param transformations the sequence of transformations
     * @return this with the specified sequence of transformations
     */
    public GeoShapeMapper transform(GeoTransformation... transformations) {
        if (this.transformations == null) {
            this.transformations = Arrays.asList(transformations);
        } else {
            this.transformations.addAll(Arrays.asList(transformations));
        }
        return this;
    }
}
