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

import com.stratio.cassandra.lucene.search.condition.GeoDistance;
import com.stratio.cassandra.lucene.search.condition.GeoTransformation;
import com.stratio.cassandra.lucene.util.Builder;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.annotate.*;

/**
 * Builder for geospatial transformations.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = GeoTransformationBuilder.Buffer.class, name = "buffer"),
               @JsonSubTypes.Type(value = GeoTransformationBuilder.Centroid.class, name = "centroid"),
               @JsonSubTypes.Type(value = GeoTransformationBuilder.Difference.class, name = "difference"),
               @JsonSubTypes.Type(value = GeoTransformationBuilder.Intersection.class, name = "intersection"),
               @JsonSubTypes.Type(value = GeoTransformationBuilder.Union.class, name = "union")})
public interface GeoTransformationBuilder<T extends GeoTransformation> extends Builder<T> {

    /**
     * {@link GeoTransformation} that gets the buffer around a JTS geographical shape.
     */
    @JsonTypeName("buffer")
    class Buffer implements GeoTransformationBuilder<GeoTransformation.Buffer> {

        /** The max allowed distance. */
        @JsonProperty("max_distance")
        public String maxDistance;

        /** The min allowed distance. */
        @JsonProperty("min_distance")
        public String minDistance;

        /**
         * Sets the max allowed distance.
         *
         * @param maxDistance the min distance
         * @return this with the specified min distance
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

        /** {@inheritDoc} */
        @Override
        public GeoTransformation.Buffer build() {
            GeoDistance min = StringUtils.isBlank(minDistance) ? null : GeoDistance.parse(minDistance);
            GeoDistance max = StringUtils.isBlank(maxDistance) ? null : GeoDistance.parse(maxDistance);
            return new GeoTransformation.Buffer(max, min);
        }

    }

    /**
     * {@link GeoTransformation} that gets the centroid of a JTS geographical shape.
     */
    @JsonTypeName("centroid")
    class Centroid implements GeoTransformationBuilder<GeoTransformation.Centroid> {

        /** {@inheritDoc} */
        @Override
        public GeoTransformation.Centroid build() {
            return new GeoTransformation.Centroid();
        }

    }

    /**
     * {@link GeoTransformation} that gets the difference of two JTS geographical shapes.
     */
    @JsonTypeName("difference")
    class Difference implements GeoTransformationBuilder<GeoTransformation.Difference> {

        /** The other shape. */
        @JsonProperty("shape")
        public final String shape;

        /**
         * Constructor receiving the geometry to be subtracted.
         *
         * @param shape the geometry to be subtracted in WKT format
         */
        @JsonCreator
        public Difference(@JsonProperty("shape") String shape) {
            this.shape = shape;
        }

        /** {@inheritDoc} */
        @Override
        public GeoTransformation.Difference build() {
            return new GeoTransformation.Difference(shape);
        }

    }

    /**
     * {@link GeoTransformation} that gets the intersection of two JTS geographical shapes.
     */
    @JsonTypeName("intersection")
    class Intersection implements GeoTransformationBuilder<GeoTransformation.Intersection> {

        /** The other shape. */
        @JsonProperty("shape")
        public final String shape;

        /**
         * Constructor receiving the geometry to be intersected.
         *
         * @param shape the geometry to be intersected in WKT format
         */
        @JsonCreator
        public Intersection(@JsonProperty("shape") String shape) {
            this.shape = shape;
        }

        /** {@inheritDoc} */
        @Override
        public GeoTransformation.Intersection build() {
            return new GeoTransformation.Intersection(shape);
        }

    }

    /**
     * {@link GeoTransformation} that gets the union of two JTS geographical shapes.
     */
    @JsonTypeName("union")
    class Union implements GeoTransformationBuilder<GeoTransformation.Union> {

        /** The other shape. */
        @JsonProperty("shape")
        public final String shape;

        /**
         * Constructor receiving the geometry to be added.
         *
         * @param shape the geometry to be added in WKT format
         */
        @JsonCreator
        public Union(@JsonProperty("shape") String shape) {
            this.shape = shape;
        }

        /** {@inheritDoc} */
        @Override
        public GeoTransformation.Union build() {
            return new GeoTransformation.Union(shape);
        }

    }
}
