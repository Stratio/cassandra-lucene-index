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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = GeoTransformationBuilder.ClipperBuilder.class, name = "clipper"),
               @JsonSubTypes.Type(value = GeoTransformationBuilder.IdentityBuilder.class, name = "identity")})
public interface GeoTransformationBuilder<T extends GeoTransformation> {

    T build();

    @JsonTypeName("identity")
    final class IdentityBuilder implements GeoTransformationBuilder<GeoTransformation.Identity> {

        @JsonCreator
        public IdentityBuilder() {
        }

        @Override
        public GeoTransformation.Identity build() {
            return new GeoTransformation.Identity();
        }
    }

    @JsonTypeName("clipper")
    final class ClipperBuilder implements GeoTransformationBuilder<GeoTransformation.Clipper> {

        protected static final Logger logger = LoggerFactory.getLogger(GeoTransformationBuilder.class);

        /** The max allowed distance. */
        @JsonProperty("max_distance")
        private final String maxDistance;

        /** The min allowed distance. */
        @JsonProperty("min_distance")
        private String minDistance;

        @JsonCreator
        public ClipperBuilder(@JsonProperty("max_distance") String maxDistance) {
            this.maxDistance = maxDistance;
        }

        public ClipperBuilder setMinDistance(String minDistance) {
            this.minDistance = minDistance;
            return this;
        }

        @Override
        public GeoTransformation.Clipper build() {
            logger.debug("MIN STRING {}", minDistance);
            logger.debug("MAX STRING {}", maxDistance);
            GeoDistance min = StringUtils.isBlank(minDistance) ? null : GeoDistance.create(minDistance);
            GeoDistance max = StringUtils.isBlank(maxDistance) ? null : GeoDistance.create(maxDistance);
            logger.debug("MIN DISTANCE {}", min);
            logger.debug("MAX DISTANCE {}", max);
            return new GeoTransformation.Clipper(max, min);
        }

    }
}
