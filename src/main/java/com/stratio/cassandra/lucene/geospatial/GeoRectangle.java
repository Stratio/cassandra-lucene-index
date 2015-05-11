/*
 * Copyright 2015, Stratio.
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
package com.stratio.cassandra.lucene.geospatial;

import com.google.common.base.Objects;
import com.spatial4j.core.context.SpatialContext;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Class representing a rectangle in geographical coordinates.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoRectangle extends GeoShape {

    private final double minLongitude;
    private final double maxLongitude;
    private final double minLatitude;
    private final double maxLatitude;

    /**
     * Builds a new {@link GeoRectangle} defined by the specified geographical positions.
     *
     * @param minLongitude The minimum longitude in the rectangle to be built.
     * @param maxLongitude The maximum longitude in the rectangle to be built.
     * @param minLatitude  The minimum latitude in the rectangle to be built.
     * @param maxLatitude  The maximum latitude in the rectangle to be built.
     */
    @JsonCreator
    public GeoRectangle(@JsonProperty("min_longitude") double minLongitude,
                        @JsonProperty("max_longitude") double maxLongitude,
                        @JsonProperty("min_latitude") double minLatitude,
                        @JsonProperty("max_latitude") double maxLatitude) {
        checkLongitude(minLongitude);
        checkLongitude(maxLongitude);
        checkLatitude(minLatitude);
        checkLatitude(maxLatitude);
        this.minLongitude = minLongitude;
        this.maxLongitude = maxLongitude;
        this.minLatitude = minLatitude;
        this.maxLatitude = maxLatitude;
    }

    /** {@inheritDoc} */
    @Override
    public com.spatial4j.core.shape.Shape toSpatial4j(SpatialContext spatialContext) {
        return spatialContext.makeRectangle(minLongitude, maxLongitude, minLatitude, maxLatitude);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("minLongitude", minLongitude)
                      .add("maxLongitude", maxLongitude)
                      .add("minLatitude", minLatitude)
                      .add("maxLatitude", maxLatitude)
                      .toString();
    }
}
