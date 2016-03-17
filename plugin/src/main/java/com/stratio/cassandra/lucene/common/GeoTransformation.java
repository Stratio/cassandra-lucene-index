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

package com.stratio.cassandra.lucene.common;

import com.google.common.base.MoreObjects;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.stratio.cassandra.lucene.util.GeospatialUtilsJTS;
import com.vividsolutions.jts.geom.Geometry;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * Class representing the transformation of a JTS geographical shape into a new shape.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = GeoTransformation.Buffer.class, name = "buffer"),
               @JsonSubTypes.Type(value = GeoTransformation.Centroid.class, name = "centroid"),
               @JsonSubTypes.Type(value = GeoTransformation.Difference.class, name = "difference"),
               @JsonSubTypes.Type(value = GeoTransformation.Intersection.class, name = "intersection"),
               @JsonSubTypes.Type(value = GeoTransformation.Union.class, name = "union")})
public interface GeoTransformation {

    /**
     * Returns the transformed {@link JtsGeometry} resulting of applying this transformation to the specified {@link
     * JtsGeometry} using the specified {@link JtsSpatialContext}.
     *
     * @param shape the JTS shape to be transformed
     * @param context the JTS spatial context to be used
     * @return the transformed JTS shape
     */
    JtsGeometry apply(JtsGeometry shape, JtsSpatialContext context);

    /**
     * {@link GeoTransformation} that returns the bounding shape of a JTS geographical shape.
     */
    class Buffer implements GeoTransformation {

        /** The max allowed distance. */
        @JsonProperty("max_distance")
        private GeoDistance maxDistance;

        /** The min allowed distance. */
        @JsonProperty("min_distance")
        private GeoDistance minDistance;

        /**
         * Sets the max allowed distance.
         *
         * @param maxDistance the min distance
         * @return this with the specified min distance
         */
        public Buffer maxDistance(GeoDistance maxDistance) {
            this.maxDistance = maxDistance;
            return this;
        }

        /**
         * Sets the min allowed distance.
         *
         * @param minDistance the min distance
         * @return this with the specified min distance
         */
        public Buffer minDistance(GeoDistance minDistance) {
            this.minDistance = minDistance;
            return this;
        }

        /**
         * Returns the max allowed distance.
         *
         * @return the max distance
         */
        public GeoDistance maxDistance() {
            return maxDistance;
        }

        /**
         * Returns the min allowed distance.
         *
         * @return the min distance
         */
        public GeoDistance minDistance() {
            return minDistance;
        }

        /**
         * Returns the buffer of the specified {@link JtsGeometry}.
         *
         * @param shape the JTS shape to be transformed
         * @param context the JTS spatial context to be used
         * @return the buffer
         */
        @Override
        public JtsGeometry apply(JtsGeometry shape, JtsSpatialContext context) {

            JtsGeometry max = maxDistance == null
                              ? context.makeShape(shape.getGeom())
                              : shape.getBuffered(maxDistance.getDegrees(), context);

            if (minDistance != null) {
                JtsGeometry min = shape.getBuffered(minDistance.getDegrees(), context);
                Geometry difference = max.getGeom().difference(min.getGeom());
                return context.makeShape(difference);
            }
            return max;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                              .add("maxDistance", maxDistance)
                              .add("minDistance", minDistance)
                              .toString();
        }
    }

    /**
     * {@link GeoTransformation} that returns the center point of a JTS geographical shape.
     */
    class Centroid implements GeoTransformation {

        /**
         * Returns the center of the specified {@link JtsGeometry}.
         *
         * @param shape the JTS shape to be transformed
         * @param context the JTS spatial context to be used
         * @return the center
         */
        @Override
        public JtsGeometry apply(JtsGeometry shape, JtsSpatialContext context) {
            Geometry centroid = shape.getGeom().getCentroid();
            return context.makeShape(centroid);
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).toString();
        }
    }

    /**
     * {@link GeoTransformation} that returns the difference of two JTS geographical shapes.
     */
    class Difference implements GeoTransformation {

        /** The shape to be subtracted. */
        @JsonProperty("shape")
        public final String other;

        /**
         * Constructor receiving the geometry to be subtracted.
         *
         * @param other the geometry
         */
        @JsonCreator
        public Difference(@JsonProperty("shape") String other) {
            this.other = other;
        }

        /**
         * Returns the difference of the specified {@link JtsGeometry}.
         *
         * @param shape the JTS shape to be transformed
         * @param context the JTS spatial context to be used
         * @return the difference
         */
        @Override
        public JtsGeometry apply(JtsGeometry shape, JtsSpatialContext context) {
            Geometry geometry = GeospatialUtilsJTS.geometryFromWKT(context, other).getGeom();
            Geometry difference = shape.getGeom().difference(geometry);
            return context.makeShape(difference);
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("other", other).toString();
        }
    }

    /**
     * {@link GeoTransformation} that returns the intersection of two JTS geographical shapes.
     */
    class Intersection implements GeoTransformation {

        /** The shape to be intersected. */
        @JsonProperty("shape")
        public final String other;

        /**
         * Constructor receiving the geometry to be added.
         *
         * @param other the geometry to be added
         */
        @JsonCreator
        public Intersection(@JsonProperty("shape") String other) {
            this.other = other;
        }

        /**
         * Returns the intersection of the specified {@link JtsGeometry}.
         *
         * @param shape the JTS shape to be transformed
         * @param context the JTS spatial context to be used
         * @return the intersection
         */
        @Override
        public JtsGeometry apply(JtsGeometry shape, JtsSpatialContext context) {
            Geometry geometry = GeospatialUtilsJTS.geometryFromWKT(context, other).getGeom();
            Geometry intersection = shape.getGeom().intersection(geometry);
            return context.makeShape(intersection);
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("other", other).toString();
        }
    }

    /**
     * {@link GeoTransformation} that returns the union of two JTS geographical shapes.
     */
    class Union implements GeoTransformation {

        /** The shape to be added. */
        @JsonProperty("shape")
        public final String other;

        /**
         * Constructor receiving the geometry to be added.
         *
         * @param other the geometry to be added
         */
        @JsonCreator
        public Union(@JsonProperty("shape") String other) {
            this.other = other;
        }

        /**
         * Returns the union of the specified {@link JtsGeometry}.
         *
         * @param shape the JTS shape to be transformed
         * @param context the JTS spatial context to be used
         * @return the union
         */
        @Override
        public JtsGeometry apply(JtsGeometry shape, JtsSpatialContext context) {
            Geometry geometry = GeospatialUtilsJTS.geometryFromWKT(context, other).getGeom();
            Geometry union = shape.getGeom().union(geometry);
            return context.makeShape(union);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("other", other).toString();
        }
    }
}
