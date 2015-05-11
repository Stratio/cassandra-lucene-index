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

import com.spatial4j.core.context.SpatialContext;
import com.stratio.cassandra.lucene.util.JsonSerializer;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.io.IOException;

/**
 * @author Andres de la Pena <adelapena@stratio.com>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = GeoPoint.class, name = "point"),
               @JsonSubTypes.Type(value = GeoRectangle.class, name = "rectangle"),
               @JsonSubTypes.Type(value = GeoCircle.class, name = "circle"),})
public abstract class GeoShape {

    /**
     * Returns the {@link GeoShape} represented by the specified JSON {@code String}.
     *
     * @param json The JSON to be parsed.
     * @return The {@link GeoShape} represented by the specified JSON {@code String}.
     */
    public static GeoShape fromJson(String json) {
        try {
            return JsonSerializer.fromString(json, GeoShape.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unparseable shape");
        }
    }

    /**
     * Returns the {@link com.spatial4j.core.shape.Shape} representation of this geographical shape.
     *
     * @param spatialContext The spatial context to be used.
     * @return The {@link com.spatial4j.core.shape.Shape} representation of this geographical shape.
     */
    public abstract com.spatial4j.core.shape.Shape toSpatial4j(SpatialContext spatialContext);

    /**
     * Throws an {@link IllegalArgumentException} if the specified value is not a valid longitude. A valid longitude
     * must in the range [-180, 180].
     *
     * @param longitude The longitude to be checked.
     */
    public static void checkLongitude(Double longitude) {
        if (longitude == null) {
            throw new IllegalArgumentException("Not null longitude required");
        }
        if (longitude < -180.0 || longitude > 180) {
            throw new IllegalArgumentException("Longitude must be between -180.0 and 180.0");
        }
    }

    /**
     * Throws an {@link IllegalArgumentException} if the specified value is not a valid latitude. A valid latitude must
     * in the range [-90, 90].
     *
     * @param latitude The latitude to be checked.
     */
    public static void checkLatitude(Double latitude) {
        if (latitude == null) {
            throw new IllegalArgumentException("Not null latitude required");
        }
        if (latitude < -90.0 || latitude > 90) {
            throw new IllegalArgumentException("Latitude must be between -90.0 and 90.0");
        }
    }
}
