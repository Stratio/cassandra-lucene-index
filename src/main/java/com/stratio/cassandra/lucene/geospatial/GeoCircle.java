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
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Shape;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Class representing a circle in geographical coordinates.
 *
 * @author Andres de la Pena <adelapena@stratio.com>
 */
public class GeoCircle extends GeoShape {

    private final double longitude; // The circle's center point longitude
    private final double latitude; // The circle's center point latitude
    private final GeoDistance distance; // The radius of the bounding circle

    /**
     * Builds a new {@link GeoCircle} centered in the point defined by the specified longitude and latitude, and with
     * the specified radius.
     *
     * @param longitude The circle's center point longitude.
     * @param latitude  The circle's center point latitude.
     * @param distance  The radius of the bounding circle.
     */
    @JsonCreator
    public GeoCircle(@JsonProperty("longitude") double longitude,
                     @JsonProperty("latitude") double latitude,
                     @JsonProperty("distance") GeoDistance distance) {
        checkLongitude(longitude);
        checkLatitude(latitude);
        this.longitude = longitude;
        this.latitude = latitude;
        this.distance = distance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Shape toSpatial4j(SpatialContext spatialContext) {
        double kms = distance.getValue(GeoDistanceUnit.KILOMETRES);
        double d = DistanceUtils.dist2Degrees(kms, DistanceUtils.EARTH_MEAN_RADIUS_KM);
        return spatialContext.makeCircle(longitude, latitude, d);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                      .add("longitude", longitude)
                      .add("latitude", latitude)
                      .add("distance", distance)
                      .toString();
    }
}
