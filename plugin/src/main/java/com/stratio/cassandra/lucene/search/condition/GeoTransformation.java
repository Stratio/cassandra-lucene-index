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

import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public interface GeoTransformation {

    JtsGeometry transform(JtsGeometry shape, JtsSpatialContext spatialContext);

    class Identity implements GeoTransformation {

        @Override
        public JtsGeometry transform(JtsGeometry shape, JtsSpatialContext spatialContext) {
            return shape;
        }
    }

    class Clipper implements GeoTransformation {

        protected static final Logger logger = LoggerFactory.getLogger(Clipper.class);

        public final GeoDistance maxDistance;
        public final GeoDistance minDistance;

        public Clipper(GeoDistance maxDistance, GeoDistance minDistance) {

            this.maxDistance = maxDistance;
            this.minDistance = minDistance;
        }

        @Override
        public JtsGeometry transform(JtsGeometry shape, JtsSpatialContext spatialContext) {

            double kms = maxDistance.getValue(GeoDistanceUnit.KILOMETRES);
            double degrees = DistanceUtils.dist2Degrees(kms, DistanceUtils.EARTH_MEAN_RADIUS_KM);
            JtsGeometry max = shape.getBuffered(degrees, spatialContext);
            logger.debug("MAX {}", max);

            if (minDistance != null) {
                kms = minDistance.getValue(GeoDistanceUnit.KILOMETRES);
                degrees = DistanceUtils.dist2Degrees(kms, DistanceUtils.EARTH_MEAN_RADIUS_KM);
                JtsGeometry min = shape.getBuffered(degrees, spatialContext);
                logger.debug("MIN {}", min);
                Geometry difference = max.getGeom().difference(min.getGeom());
                logger.debug("DIFFERENCE {}", difference);
                return spatialContext.makeShape(difference);
            }
            return max;
        }
    }
}
