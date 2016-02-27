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

package com.stratio.cassandra.lucene.builder.search.condition;


import com.stratio.cassandra.lucene.builder.Builder;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = GeoTransformation.Buffer.class, name = "buffer")})
public abstract class GeoTransformation extends Builder {

    /**
     * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
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
    }

}

