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

package com.stratio.cassandra.lucene.search.condition;

import com.google.common.base.Objects;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.stratio.cassandra.lucene.util.GeospatialUtils;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Class representing the transformation of a JTS geographical shape into a new shape.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
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
     * {@link GeoTransformation} for getting the bounding shape of a JTS geographical shape.
     */
    class Buffer implements GeoTransformation {

        public final GeoDistance maxDistance;
        public final GeoDistance minDistance;

        /**
         * Constructor receiving the max and minimum accepted distances.
         *
         * @param maxDistance the max accepted distance
         * @param minDistance the min accepted distance
         */
        public Buffer(GeoDistance maxDistance, GeoDistance minDistance) {
            this.maxDistance = maxDistance;
            this.minDistance = minDistance;
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
            return Objects.toStringHelper(this)
                          .add("maxDistance", maxDistance)
                          .add("minDistance", minDistance)
                          .toString();
        }
    }

    /**
     * {@link GeoTransformation} that gets the center point of a JTS geographical shape.
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
            return Objects.toStringHelper(this).toString();
        }
    }

    /**
     * {@link GeoTransformation} that gets the difference of two JTS geographical shapes.
     */
    class Difference implements GeoTransformation {

        public final String other;

        /**
         * Constructor receiving the geometry to be subtracted.
         *
         * @param other the geometry
         */
        public Difference(String other) {
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
            Geometry geometry = GeospatialUtils.geometryFromWKT(context, other).getGeom();
            Geometry difference = shape.getGeom().difference(geometry);
            return context.makeShape(difference);
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("other", other).toString();
        }
    }

    /**
     * {@link GeoTransformation} that gets the intersection of two JTS geographical shapes.
     */
    class Intersection implements GeoTransformation {

        public final String other;

        /**
         * Constructor receiving the geometry to be added.
         *
         * @param other the geometry to be added
         */
        public Intersection(String other) {
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
            Geometry geometry = GeospatialUtils.geometryFromWKT(context, other).getGeom();
            Geometry intersection = shape.getGeom().intersection(geometry);
            return context.makeShape(intersection);
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("other", other).toString();
        }
    }

    /**
     * {@link GeoTransformation} that gets the union of two JTS geographical shapes.
     */
    class Union implements GeoTransformation {

        public final String other;

        /**
         * Constructor receiving the geometry to be added.
         *
         * @param other the geometry to be added
         */
        public Union(String other) {
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
            Geometry geometry = GeospatialUtils.geometryFromWKT(context, other).getGeom();
            Geometry union = shape.getGeom().union(geometry);
            return context.makeShape(union);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("other", other).toString();
        }
    }
}
