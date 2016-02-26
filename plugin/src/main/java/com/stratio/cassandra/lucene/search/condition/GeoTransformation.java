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
     * {@link GeoTransformation} for getting copying of a JTS geographical shape.
     */
    class Copy implements GeoTransformation {

        /**
         * Returns a copy of the specified {@link JtsGeometry}.
         *
         * @param shape the JTS shape to be copied
         * @param context the JTS spatial context to be used
         * @return the copy of the JTS shape
         */
        @Override
        public JtsGeometry apply(JtsGeometry shape, JtsSpatialContext context) {
            return context.makeShape(shape.getGeom());
        }
    }

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

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                          .add("maxDistance", maxDistance)
                          .add("minDistance", minDistance)
                          .toString();
        }
    }
}
