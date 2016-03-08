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

package com.stratio.cassandra.lucene.builder.common;

import com.stratio.cassandra.lucene.builder.Builder;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = GeoTransformation.Buffer.class, name = "buffer"),
               @JsonSubTypes.Type(value = GeoTransformation.Centroid.class, name = "centroid"),
               @JsonSubTypes.Type(value = GeoTransformation.Difference.class, name = "difference"),
               @JsonSubTypes.Type(value = GeoTransformation.Intersection.class, name = "intersection"),
               @JsonSubTypes.Type(value = GeoTransformation.Union.class, name = "union")})
public abstract class GeoTransformation extends Builder {

    /**
     * {@link GeoTransformation} for getting the bounding shape of a JTS geographical shape.
     */
    public static class Buffer extends GeoTransformation {

        /** The max allowed distance. */
        @JsonProperty("max_distance")
        String maxDistance;

        /** The min allowed distance. */
        @JsonProperty("min_distance")
        String minDistance;

        /**
         * Sets the max allowed distance.
         *
         * @param maxDistance the max distance
         * @return this with the specified max distance
         */
        public Buffer maxDistance(String maxDistance) {
            this.maxDistance = maxDistance;
            return this;
        }

        /**
         * Sets the min allowed distance.
         *
         * @param minDistance the min distance
         * @return this with the specified min distance
         */
        public Buffer minDistance(String minDistance) {
            this.minDistance = minDistance;
            return this;
        }
    }

    /**
     * {@link GeoTransformation} that gets the center point of a JTS geographical shape.
     */
    public static class Centroid extends GeoTransformation {

    }

    /**
     * {@link GeoTransformation} that gets the difference of two JTS geographical shapes.
     */
    public static class Difference extends GeoTransformation {

        /** The other shape. */
        @JsonProperty("shape")
        public final String shape;

        /**
         * Constructor receiving the geometry to be subtracted.
         *
         * @param shape the geometry to be subtracted in WKT format
         */
        public Difference(String shape) {
            this.shape = shape;
        }

    }

    /**
     * {@link GeoTransformation} that gets the intersection of two JTS geographical shapes.
     */
    public static class Intersection extends GeoTransformation {

        /** The other shape. */
        @JsonProperty("shape")
        public final String shape;

        /**
         * Constructor receiving the geometry to be intersected.
         *
         * @param shape the geometry to be intersected in WKT format
         */
        public Intersection(String shape) {
            this.shape = shape;
        }

    }

    /**
     * {@link GeoTransformation} that gets the union of two JTS geographical shapes.
     */
    public static class Union extends GeoTransformation {

        /** The other shape. */
        @JsonProperty("shape")
        public final String shape;

        /**
         * Constructor receiving the geometry to be added.
         *
         * @param shape the geometry to be added in WKT format
         */
        public Union(String shape) {
            this.shape = shape;
        }

    }

}

